"""
Phase 2 + Phase 3 Celery orchestration.

Uses Celery signatures by task name (not importing paper_tasks) to avoid:
  paper_tasks → paper_worker → phase_orchestration → paper_tasks
"""

from __future__ import annotations

import copy
import logging
from collections import defaultdict

from celery import group, signature

from app.pipeline.answer_generator import (
    build_answer_batches,
    normalize_answer_list,
)
from app.tasks.dispatch import run_task_group

logger = logging.getLogger("app.phase_orchestration")


def _attach_answers_to_phase2(phase2_result: dict, answer_list: list) -> dict:
    chunk_answers = {}
    for answer_obj in normalize_answer_list(answer_list):
        q_num = str(answer_obj.get("question_number", ""))
        if not q_num:
            continue
        # DEDUPE (first-wins): the model occasionally emits the same
        # question_number twice in one batch. Previously the dict build
        # silently let the LAST occurrence win (undefined behavior). Now the
        # FIRST occurrence wins and duplicates are logged loudly.
        if q_num in chunk_answers:
            logger.warning(
                f"[_attach_answers_to_phase2] Duplicate answer for question_number "
                f"'{q_num}' in the same chunk — keeping the FIRST, ignoring duplicate."
            )
            continue
        chunk_answers[q_num] = {
            "answer_data": answer_obj.get("answer_data", {}),
        }
    phase2_result["_answers"] = chunk_answers
    return phase2_result


# ---------------------------------------------------------------------------
# Self-healing recovery passes (additive — run AFTER normal Phase 3 batches).
# Both are strictly best-effort: any failure inside them is logged and
# swallowed, so the pipeline NEVER behaves worse than before they existed.
# ---------------------------------------------------------------------------

def _recover_missing_answers(phase2_results: list, config: dict) -> None:
    """
    RECOVERY PASS 1 — questions with NO answer object at all.

    Previously: if an answer batch failed permanently (or the model skipped a
    question), zip_answers_into_questions() silently shipped answer_text: "".
    Now: collect every question_number missing from _answers and re-run those
    questions in ONE recovery call per chunk (in-process, not via Celery, so
    no nested group waits). Existing answers are NEVER overwritten.
    """
    from app.pipeline.answer_generator import generate_answers_for_batch

    for phase2_result in phase2_results:
        chunk_index = phase2_result.get("_chunk_index", "?")
        try:
            chunk_answers = phase2_result.get("_answers", {}) or {}
            questions = phase2_result.get("questions", []) or []
            missing = [
                q for q in questions
                if str(q.get("question_number", "")) not in chunk_answers
            ]
            if not missing:
                continue

            missing_nums = [str(q.get("question_number")) for q in missing]
            logger.warning(
                f"[Phase3-Recovery-Chunk{chunk_index}] {len(missing)} question(s) "
                f"have no answer after normal batches: {missing_nums} — "
                f"running one recovery call."
            )
            recovered = generate_answers_for_batch(
                missing, config, chunk_index, "RECOVERY", 1, 1
            )
            added = 0
            for answer_obj in normalize_answer_list(recovered):
                q_num = str(answer_obj.get("question_number", ""))
                # Only fill numbers that are STILL missing — never overwrite.
                if q_num and q_num in missing_nums and q_num not in chunk_answers:
                    chunk_answers[q_num] = {
                        "answer_data": answer_obj.get("answer_data", {}),
                    }
                    added += 1
            phase2_result["_answers"] = chunk_answers
            logger.info(
                f"[Phase3-Recovery-Chunk{chunk_index}] Recovered {added}/"
                f"{len(missing)} missing answer(s)."
            )
        except Exception as exc:
            logger.error(
                f"[Phase3-Recovery-Chunk{chunk_index}] Recovery pass failed "
                f"({type(exc).__name__}: {exc}) — continuing with existing answers."
            )


def _strip_alternatives_deep(entry: dict) -> None:
    """Remove every ALTERNATIVE from additional_questions /
    child_additional_questions of a (deep-copied) question entry, in place."""
    for key in ("additional_questions", "child_additional_questions"):
        entries = entry.get(key)
        if not entries:
            continue
        kept = [
            e for e in entries
            if e.get("additional_question_type") != "ALTERNATIVE"
        ]
        entry[key] = kept
        for child in kept:
            _strip_alternatives_deep(child)


def _repair_unanswered_roots(phase2_results: list, config: dict) -> None:
    """
    RECOVERY PASS 2 — the confirmed "root left unanswered because it has an
    ALTERNATIVE" bug (see postprocess.flag_unanswered_root_with_alternative,
    which previously only LOGGED it and shipped a blank root answer).

    Detection here (before is_correct resolution): the question has an
    ALTERNATIVE sibling, has NO SUB_QUESTION siblings (a root with subparts
    legitimately keeps answer_text == ""), and its answer_text is empty.

    Repair: deep-copy the question, strip every ALTERNATIVE out of it (the
    ALTERNATIVE is exactly what confuses the model into skipping the root),
    and run ONE tiny single-question call. Only the empty answer_text is
    filled from the repair — the already-generated additional_answers
    (including the ALTERNATIVE's own answer) are kept untouched.
    """
    from app.pipeline.answer_generator import generate_answers_for_batch

    for phase2_result in phase2_results:
        chunk_index = phase2_result.get("_chunk_index", "?")
        chunk_answers = phase2_result.get("_answers", {}) or {}

        for q in phase2_result.get("questions", []) or []:
            try:
                q_num = str(q.get("question_number", ""))
                q_data = q.get("question_data", {}) or {}
                add_qs = q_data.get("additional_questions") or []
                has_alternative = any(
                    a.get("additional_question_type") == "ALTERNATIVE"
                    for a in add_qs
                )
                has_sub_question = any(
                    a.get("additional_question_type") == "SUB_QUESTION"
                    for a in add_qs
                )
                if not has_alternative or has_sub_question:
                    continue

                entry = chunk_answers.get(q_num)
                a_data = (entry or {}).get("answer_data", {}) or {}
                if str(a_data.get("answer_text", "")).strip():
                    continue  # root already answered — nothing to repair

                logger.warning(
                    f"[Phase3-RootRepair-Chunk{chunk_index}] Q{q_num}: root has an "
                    f"ALTERNATIVE but its own answer_text is empty — running a "
                    f"single-question repair call (ALTERNATIVE stripped)."
                )
                stripped_q = copy.deepcopy(q)
                stripped_data = stripped_q.get("question_data", {}) or {}
                _strip_alternatives_deep(stripped_data)

                repaired = generate_answers_for_batch(
                    [stripped_q], config, chunk_index,
                    f"ROOT_REPAIR_Q{q_num}", 1, 1,
                )
                repaired = normalize_answer_list(repaired)
                new_text = ""
                for r in repaired:
                    if str(r.get("question_number", "")) == q_num:
                        new_text = str(
                            (r.get("answer_data", {}) or {}).get("answer_text", "")
                        ).strip()
                        break

                if new_text:
                    if entry is None:
                        entry = {"answer_data": {"answer_text": ""}}
                        chunk_answers[q_num] = entry
                    entry.setdefault("answer_data", {})
                    entry["answer_data"]["answer_text"] = new_text
                    logger.info(
                        f"[Phase3-RootRepair-Chunk{chunk_index}] Q{q_num}: root "
                        f"answer_text repaired ({len(new_text)} chars)."
                    )
                else:
                    logger.error(
                        f"[Phase3-RootRepair-Chunk{chunk_index}] Q{q_num}: repair "
                        f"call returned no usable answer_text — root stays flagged "
                        f"for manual review (existing behavior)."
                    )
            except Exception as exc:
                logger.error(
                    f"[Phase3-RootRepair-Chunk{chunk_index}] Repair failed for one "
                    f"question ({type(exc).__name__}: {exc}) — continuing."
                )

        phase2_result["_answers"] = chunk_answers


def run_phase3_for_all_chunks(
    phase2_results: list, config: dict, job_id: str
) -> list:
    """
    Fan out ALL answer batches across ALL chunks in one Celery group.
    Must be called from the parent task (not from a chunk worker) to avoid
    nested wait deadlocks when worker concurrency is small.

    Individual batch failures (after retries) do not abort the group — they
    return a failed payload and remaining batches continue.
    """
    batch_specs: list[dict] = []
    for phase2_result in phase2_results:
        chunk_index = phase2_result.get("_chunk_index", "?")
        questions = phase2_result.get("questions", [])
        if not questions:
            logger.info(f"[Phase3-Chunk{chunk_index}] No questions to answer — skipping.")
            continue
        batches = build_answer_batches(questions)
        type_counts: dict = {}
        for b in batches:
            type_counts[b["qtype"]] = type_counts.get(b["qtype"], 0) + len(b["questions"])
        logger.info(
            f"[Phase3-Chunk{chunk_index}] Answer generation for {len(questions)} question(s) "
            f"across {len(batches)} batch(es): {type_counts}"
        )
        for batch in batches:
            batch_specs.append(
                {
                    "chunk_index": chunk_index,
                    "questions": batch["questions"],
                    "qtype": batch["qtype"],
                    "batch_num": batch["batch_num"],
                    "num_batches": batch["num_batches"],
                }
            )

    if not batch_specs:
        for phase2_result in phase2_results:
            phase2_result.setdefault("_answers", {})
        return phase2_results

    logger.info(
        f"Submitting {len(batch_specs)} answer batch(es) to Celery group "
        f"(across {len(phase2_results)} chunk(s))."
    )
    answer_group = group(
        signature(
            "paper.process_answer_batch",
            args=(
                job_id,
                spec["questions"],
                config,
                spec["chunk_index"],
                spec["qtype"],
                spec["batch_num"],
                spec["num_batches"],
            ),
        )
        for spec in batch_specs
    )
    batch_results = run_task_group(answer_group)

    answers_by_chunk: dict = defaultdict(list)
    failed_batches: list[str] = []
    for spec, result in zip(batch_specs, batch_results):
        chunk_index = spec["chunk_index"]
        label = (
            f"Chunk {chunk_index} [{spec['qtype']}] "
            f"batch {spec['batch_num']}/{spec['num_batches']}"
        )

        if isinstance(result, dict):
            status = result.get("status", "ok")
            if status == "failed":
                failed_batches.append(f"{label}: {result.get('error')}")
                logger.error(
                    f"[Phase3] Batch failed permanently — {label}: {result.get('error')}"
                )
                continue
            answers_by_chunk[chunk_index].extend(
                normalize_answer_list(result.get("answers"))
            )
        else:
            answers_by_chunk[chunk_index].extend(normalize_answer_list(result))

    if failed_batches:
        logger.warning(
            f"[Phase3] {len(failed_batches)} answer batch(es) failed after retries; "
            f"continuing with successful batches. Failures: {failed_batches}"
        )

    for phase2_result in phase2_results:
        chunk_index = phase2_result.get("_chunk_index", "?")
        _attach_answers_to_phase2(
            phase2_result, answers_by_chunk.get(chunk_index, [])
        )
        logger.info(
            f"[Phase3-Chunk{chunk_index}] Answer generation done — "
            f"{len(phase2_result.get('_answers', {}))} answer(s) attached."
        )

    # Self-healing passes (additive, best-effort — see helper docstrings).
    _recover_missing_answers(phase2_results, config)
    _repair_unanswered_roots(phase2_results, config)

    return phase2_results


def run_phase2_and_phase3_all(chunks: list, config: dict, job_id: str):
    """
    CRITICAL DESIGN NOTE — answers are CHUNK-SCOPED, never global.

    Orchestration (single level of Celery waits — no nesting):
      1. Fan out Phase 2 extract per chunk, wait.
      2. Fan out Phase 3 answer batches across all chunks, wait.
      3. Attach chunk-scoped _answers and return.
    """
    if not chunks:
        return [], None

    logger.info(f"Submitting {len(chunks)} chunk(s) to Celery group (Phase 2 only).")
    chunk_group = group(
        signature(
            "paper.process_chunk_p2",
            args=(job_id, chunk, config),
        )
        for chunk in chunks
    )
    phase2_results = run_task_group(chunk_group)

    for phase2_result in phase2_results:
        chunk_index = phase2_result.get("_chunk_index", "?")
        logger.info(
            f"[Phase2-Chunk{chunk_index}] Completed — "
            f"{len(phase2_result.get('questions', []))} questions."
        )

    phase2_results = run_phase3_for_all_chunks(phase2_results, config, job_id)

    phase2_results.sort(key=lambda r: r["_chunk_index"])
    total_answers = sum(len(r.get("_answers", {})) for r in phase2_results)
    logger.info(
        f"All chunks processed — {len(phase2_results)} result(s), "
        f"{total_answers} answer(s) indexed (chunk-scoped)."
    )
    return phase2_results, None