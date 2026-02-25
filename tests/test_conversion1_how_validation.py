from __future__ import annotations

import unittest

from app.web.routes import _resolve_conversion1_how_codes


class Conversion1HowValidationTests(unittest.TestCase):
    def test_save_accepts_when_both_existing_and_generated_codes_present(self) -> None:
        # Ensure save accepts the intended workflow: existing process code + generated processing code.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="CD",
            submit_action="save",
        )

        # Validate no save-time validation errors are produced when both required fields are present.
        self.assertEqual(errors, [])
        # Validate generated processing code remains the canonical code used for persistence.
        self.assertEqual(resolved_processing_code, "CD")

    def test_save_rejects_missing_existing_process_code(self) -> None:
        # Ensure save rejects entries that are missing the manually entered existing process code.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="CD",
            submit_action="save",
        )

        # Validate save-specific process-code-required error is returned.
        self.assertIn("Use Existing Process Code is required.", errors)
        # Validate generated processing code is still preserved for deterministic form re-rendering.
        self.assertEqual(resolved_processing_code, "CD")

    def test_save_rejects_missing_generated_processing_code(self) -> None:
        # Ensure save requires the generated new processing code from the generate action.
        errors, resolved_processing_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="",
            submit_action="save",
        )

        # Validate save-specific generated-code-required error is returned.
        self.assertIn("Generate New Process Code is required. Click Generate code first.", errors)
        # Validate no generated processing code is available when the field is empty.
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
