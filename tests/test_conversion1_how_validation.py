from __future__ import annotations

import unittest

from app.web.routes import _resolve_conversion1_how_codes


class Conversion1HowValidationTests(unittest.TestCase):
    def test_save_accepts_process_code_only(self) -> None:
        # Ensure save accepts process-code-only entry and maps it into effective processing code.
        errors, resolved_processing_code, persisted_process_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="",
            submit_action="save",
        )

        # Validate no either/or errors are raised when exactly one field is entered.
        self.assertEqual(errors, [])
        # Validate process code is used as the effective processing code for persistence.
        self.assertEqual(resolved_processing_code, "AB")
        # Validate process code column is intentionally left empty in storage for mutual exclusivity.
        self.assertIsNone(persisted_process_code)

    def test_save_accepts_processing_code_only(self) -> None:
        # Ensure save accepts processing-code-only entry and keeps process_code value empty.
        errors, resolved_processing_code, persisted_process_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="CD",
            submit_action="save",
        )

        # Validate no either/or errors are raised when exactly one field is entered.
        self.assertEqual(errors, [])
        # Validate processing code stays the effective code written with the saved row.
        self.assertEqual(resolved_processing_code, "CD")
        # Validate process_code remains empty when process field is not used.
        self.assertIsNone(persisted_process_code)

    def test_save_rejects_both_codes(self) -> None:
        # Ensure save rejects submissions where both mutually-exclusive code fields are filled.
        errors, resolved_processing_code, persisted_process_code = _resolve_conversion1_how_codes(
            process_code="AB",
            processing_code="CD",
            submit_action="save",
        )

        # Validate the mutual exclusivity validation message is returned.
        self.assertIn("Only one field can be completed: Process code or Processing code.", errors)
        # Validate processing code is still resolved for predictable error-page rendering.
        self.assertEqual(resolved_processing_code, "CD")
        # Validate process code is preserved only in transient form state when both are entered.
        self.assertEqual(persisted_process_code, "AB")

    def test_save_rejects_missing_both_codes(self) -> None:
        # Ensure save enforces at least one code when neither process nor processing code is entered.
        errors, resolved_processing_code, persisted_process_code = _resolve_conversion1_how_codes(
            process_code="",
            processing_code="",
            submit_action="save",
        )

        # Validate one-of-two requirement message is shown for empty submissions.
        self.assertIn("Either Process code or Processing code is required.", errors)
        # Validate no effective processing code is produced when both inputs are empty.
        self.assertEqual(resolved_processing_code, "")
        # Validate no process-code value is staged for persistence when both inputs are empty.
        self.assertIsNone(persisted_process_code)


if __name__ == "__main__":
    unittest.main()
