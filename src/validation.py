"""
Validates a pandas DataFrame row-by-row against a Pydantic schema.
Returns (good_df, quarantine_df) - quarantine_df carries the reason so
data stewards can act on it (this is written to audit.quarantine table).
"""
import pandas as pd
from pydantic import ValidationError
from typing import Type
from src.observability import get_logger

log = get_logger("validation")


def validate_dataframe(df: pd.DataFrame, schema: Type, entity_name: str):
    good_rows = []
    bad_rows = []

    # Empty CSV cells surface as NaN (float) via pandas - Pydantic rejects that
    # for Optional[str] fields, so normalize NaN -> None before validating.
    df = df.astype(object).where(pd.notnull(df), None)

    for record in df.to_dict(orient="records"):
        try:
            validated = schema(**record)
            good_rows.append(validated.model_dump())
        except ValidationError as e:
            bad_rows.append({
                **record,
                "rejection_reason": "; ".join(
                    f"{err['loc']}: {err['msg']}" for err in e.errors()
                ),
                "entity": entity_name,
            })

    good_df = pd.DataFrame(good_rows) if good_rows else pd.DataFrame(columns=df.columns)
    bad_df = pd.DataFrame(bad_rows) if bad_rows else pd.DataFrame()

    total = len(df)
    rejected = len(bad_df)
    rate = (rejected / total) if total else 0.0
    log.info(f"{entity_name}: validated {total} rows, {rejected} rejected ({rate:.1%})")

    return good_df, bad_df, rate
