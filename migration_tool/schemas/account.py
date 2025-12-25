"""
Pydantic Schemas for account.account and account.move

Validation schemas for Chart of Accounts and Journal Entries.
"""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import date


# Odoo account types (v16+)
AccountType = Literal[
    "asset_receivable",
    "asset_cash",
    "asset_current",
    "asset_non_current",
    "asset_prepayments",
    "asset_fixed",
    "liability_payable",
    "liability_credit_card",
    "liability_current",
    "liability_non_current",
    "equity",
    "equity_unaffected",
    "income",
    "income_other",
    "expense",
    "expense_depreciation",
    "expense_direct_cost",
    "off_balance",
]


class AccountBaseSchema(BaseModel):
    """Base schema for account.account fields."""
    
    name: str = Field(..., min_length=1, max_length=256, description="Account name")
    code: str = Field(..., min_length=1, max_length=64, description="Account code")
    
    # Type
    account_type: AccountType | str = Field(
        ..., description="Account type"
    )
    
    # Settings
    reconcile: bool = Field(
        default=False, description="Allow reconciliation"
    )
    deprecated: bool = Field(
        default=False, description="Deprecated account"
    )
    
    # Currency
    currency_id: int | str | None = Field(
        default=None, description="Account currency (ID or name)"
    )
    
    # Group
    group_id: int | str | None = Field(
        default=None, description="Account group (ID or code)"
    )
    
    # Company
    company_id: int | str | None = Field(
        default=None, description="Company (ID or name)"
    )
    
    # Notes
    note: str | None = Field(default=None, description="Internal notes")
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Account name is required")
        name = str(v).strip()
        if not name:
            raise ValueError("Account name cannot be empty")
        return name
    
    @field_validator("code", mode="before")
    @classmethod
    def validate_code(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Account code is required")
        code = str(v).strip()
        if not code:
            raise ValueError("Account code cannot be empty")
        return code

    model_config = {
        "extra": "allow",
    }


class AccountSchema(AccountBaseSchema):
    """Full account schema including read-only fields."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    current_balance: float | None = Field(default=None, description="Current balance")


class AccountCreateSchema(AccountBaseSchema):
    """Schema for creating new accounts."""
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Odoo create/write."""
        data = self.model_dump(exclude_none=True)
        return data


class JournalEntryLineSchema(BaseModel):
    """Schema for account.move.line (journal entry lines)."""
    
    name: str = Field(..., description="Line description/label")
    account_id: int | str = Field(..., description="Account (ID or code)")
    
    # Amounts
    debit: float = Field(default=0.0, ge=0, description="Debit amount")
    credit: float = Field(default=0.0, ge=0, description="Credit amount")
    
    # Optional fields
    partner_id: int | str | None = Field(
        default=None, description="Partner (ID or name)"
    )
    currency_id: int | str | None = Field(
        default=None, description="Currency (ID or name)"
    )
    amount_currency: float = Field(
        default=0.0, description="Amount in foreign currency"
    )
    
    # Analytic
    analytic_distribution: dict[str, float] | None = Field(
        default=None, description="Analytic distribution"
    )
    
    @model_validator(mode="after")
    def validate_debit_credit(self) -> "JournalEntryLineSchema":
        """Ensure only debit or credit is set, not both."""
        if self.debit > 0 and self.credit > 0:
            raise ValueError("A line cannot have both debit and credit")
        return self

    model_config = {
        "extra": "allow",
    }


class JournalEntrySchema(BaseModel):
    """
    Schema for account.move (journal entries).
    
    ⚠️ WARNING: Journal entries affect financial data.
    Use with extreme caution and proper review.
    """
    
    # Header
    ref: str | None = Field(default=None, max_length=256, description="Reference")
    date: str | date = Field(..., description="Accounting date (YYYY-MM-DD)")
    
    # Journal
    journal_id: int | str = Field(..., description="Journal (ID or code)")
    
    # Type
    move_type: Literal["entry", "out_invoice", "in_invoice", "out_refund", "in_refund"] = Field(
        default="entry", description="Move type"
    )
    
    # Partner
    partner_id: int | str | None = Field(
        default=None, description="Partner (ID or name)"
    )
    
    # Currency
    currency_id: int | str | None = Field(
        default=None, description="Currency (ID or name)"
    )
    
    # Company
    company_id: int | str | None = Field(
        default=None, description="Company (ID or name)"
    )
    
    # Lines
    line_ids: list[JournalEntryLineSchema] = Field(
        ..., min_length=1, description="Journal entry lines"
    )
    
    @field_validator("date", mode="before")
    @classmethod
    def validate_date(cls, v: Any) -> str:
        if isinstance(v, date):
            return v.strftime("%Y-%m-%d")
        if isinstance(v, str):
            # Validate format
            try:
                date.fromisoformat(v)
                return v
            except ValueError:
                raise ValueError(f"Invalid date format: {v}. Use YYYY-MM-DD")
        raise ValueError("Date is required")
    
    @model_validator(mode="after")
    def validate_balanced(self) -> "JournalEntrySchema":
        """Ensure the entry is balanced (debits = credits)."""
        total_debit = sum(line.debit for line in self.line_ids)
        total_credit = sum(line.credit for line in self.line_ids)
        
        if abs(total_debit - total_credit) > 0.01:  # Allow small rounding difference
            raise ValueError(
                f"Journal entry is not balanced. "
                f"Debit: {total_debit}, Credit: {total_credit}"
            )
        return self
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for Odoo create.
        
        Formats line_ids in Odoo's command format.
        """
        data = self.model_dump(exclude={"line_ids"}, exclude_none=True)
        
        # Format lines as Odoo commands: [(0, 0, {...}), ...]
        data["line_ids"] = [
            (0, 0, line.model_dump(exclude_none=True))
            for line in self.line_ids
        ]
        
        return data

    model_config = {
        "extra": "allow",
    }


class AccountImportRow(BaseModel):
    """Schema for validating raw account import data."""
    
    # Common column name aliases
    account_name: str | None = Field(default=None, alias="Account Name")
    account_code: str | None = Field(default=None, alias="Account Code")
    account_type: str | None = Field(default=None, alias="Account Type")
    allow_reconciliation: bool | None = Field(default=None, alias="Allow Reconciliation")

    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }
