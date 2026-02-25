from __future__ import annotations

import unittest

from app.web.routes import _resolve_conversion1_how_codes


class Conversion1HowValidationTests(unittest.TestCase):
    def test_save_prefers_generated_processing_code_when_both_codes_present(self) -> None:
        # Ensure save keeps the generated processing code as canonical when both fields are present.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="CD",
            submit_action="save",
        )

        # Validate no save-time validation errors are produced when at least one code source is present.
        self.assertEqual(errors, [])
        # Validate generated processing code remains the canonical code used for persistence.
        self.assertEqual(resolved_processing_code, "CD")

    def test_save_accepts_existing_process_code_without_generated_code(self) -> None:
        # Ensure save can proceed when users choose to reuse an existing process code only.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="",
            submit_action="save",
        )

        # Validate save supports the existing-code path without requiring a generated code.
        self.assertEqual(errors, [])
        # Validate existing process code becomes the resolved processing token for persistence.
        self.assertEqual(resolved_processing_code, "AB")

    def test_save_accepts_generated_code_without_existing_process_code(self) -> None:
        # Ensure save can proceed when users generate a new processing code and leave existing code blank.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="CD",
            submit_action="save",
        )

        # Validate save supports the generated-code path without requiring existing code input.
        self.assertEqual(errors, [])
        # Validate generated processing code is preserved for deterministic save behavior.
        self.assertEqual(resolved_processing_code, "CD")

    def test_save_rejects_when_both_existing_and_generated_codes_missing(self) -> None:
        # Ensure save rejects rows only when both code sources are empty.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="",
            submit_action="save",
        )

        # Validate a single combined error instructs users that either workflow is acceptable.
        self.assertIn("Either Use Existing Process Code or Generate New Process Code is required.", errors)
        # Validate resolved code remains empty when neither input path is supplied.
        self.assertEqual(resolved_processing_code, "")

    def test_generate_allows_empty_code_inputs(self) -> None:
        # Ensure generate action does not require prefilled process/processing fields.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="",
            submit_action="generate",
        )

        # Validate generate flow can proceed without save-only required-code errors.
        self.assertEqual(errors, [])
        # Validate no processing code is pre-resolved before generate allocation runs.
        self.assertEqual(resolved_processing_code, "")


if __name__ == "__main__":
    unittest.main()
