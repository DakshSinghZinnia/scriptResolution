#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any, Dict


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Required JSON file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def merge_docinfo(letter_data: Dict[str, Any], docinfo_data: Dict[str, Any]) -> None:
    """
    Replace LetterData.DocInfo with the DocInfo object from docInfo.json.
    """
    docinfo = docinfo_data.get("DocInfo")
    if isinstance(docinfo, dict):
        letter_data["DocInfo"] = docinfo


def merge_pdf_insert(letter_data: Dict[str, Any], pdf_insert_data: Dict[str, Any]) -> None:
    """
    Replace LetterData.PDF_Insert with the PDF_Insert array from pdf_insert.json.
    """
    pdf_list = pdf_insert_data.get("PDF_Insert")
    if isinstance(pdf_list, list):
        letter_data["PDF_Insert"] = pdf_list


def merge_option_list(letter_data: Dict[str, Any], option_list_data: Dict[str, Any]) -> None:
    """
    Replace LetterData.OptionList with the OptionList array from optionList.json.
    """
    opt_list = option_list_data.get("OptionList")
    if isinstance(opt_list, list):
        letter_data["OptionList"] = opt_list


def merge_ui_fields(letter_data: Dict[str, Any], ui_data: Dict[str, Any]) -> None:
    """
    For every key in ui.json, set LetterData[key] = ui_value.
    This overwrites existing values or adds new keys if not present.
    """
    for key, value in ui_data.items():
        letter_data[key] = value


def escape_slashes(obj: Any) -> Any:
    """
    Recursively walk the structure and replace '/' with '//'
    in all string values.
    """
    if isinstance(obj, dict):
        return {k: escape_slashes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [escape_slashes(item) for item in obj]
    elif isinstance(obj, str):
        return obj.replace("/", "//")
    else:
        return obj


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "input"
    output_dir = base_dir / "output"

    # Paths to input files
    input_json_path = input_dir / "input.json"
    docinfo_json_path = input_dir / "docInfo.json"
    pdf_insert_json_path = input_dir / "pdf_insert.json"
    ui_json_path = input_dir / "ui.json"
    optionlist_json_path = input_dir / "optionList.json"

    # Load JSONs
    base_data = load_json(input_json_path)
    docinfo_data = load_json(docinfo_json_path)
    pdf_insert_data = load_json(pdf_insert_json_path)
    ui_data = load_json(ui_json_path)
    optionlist_data = load_json(optionlist_json_path)

    # Ensure LetterData exists
    if "LetterData" not in base_data or not isinstance(base_data["LetterData"], dict):
        raise ValueError("input.json must contain a top-level 'LetterData' object.")

    letter_data = base_data["LetterData"]

    # Merge pieces
    merge_docinfo(letter_data, docinfo_data)
    merge_pdf_insert(letter_data, pdf_insert_data)
    merge_option_list(letter_data, optionlist_data)
    merge_ui_fields(letter_data, ui_data)

    # Escape '/' as '//' in all string values
    output_data = escape_slashes(base_data)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "output.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"Resolved JSON written to: {output_path}")


if __name__ == "__main__":
    main()
