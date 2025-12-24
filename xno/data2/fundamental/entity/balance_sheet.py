from dataclasses import dataclass
from typing import Self

from sqlalchemy import Row


@dataclass(frozen=True)
class BalanceSheet:

    ticker: str
    fiscal_quarter: int
    fiscal_year: int

    _WIGROUP_MAP = {
        "code": "ticker",
        "quy": "fiscal_quarter",
        "nam": "fiscal_year",
    }

    @classmethod
    def from_wigroup(cls, raw: dict | Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        mapped_data = {}
        for key, value in raw.items():
            if key in cls._WIGROUP_MAP:
                mapped_key = cls._WIGROUP_MAP[key]
                mapped_data[mapped_key] = value
        return cls(**mapped_data)


# CURRENT ASSETS (TÀI SẢN NGẮN HẠN)
@dataclass(frozen=True)
class BalanceSheetShortTerm(BalanceSheet):

    current_assets_section: int | None = None
    cash_and_equivalents_section: int | None = None
    cash: int | None = None
    cash_equivalents: int | None = None
    short_term_financial_investments_section: int | None = None
    trading_securities: int | None = None
    trading_securities_allowance: int | None = None
    held_to_maturity_st: int | None = None
    receivables_short_term_section: int | None = None
    accounts_receivable_st: int | None = None
    advances_to_suppliers_st: int | None = None
    intercompany_receivables_st: int | None = None
    construction_receivables: int | None = None
    loans_receivable_st: int | None = None
    other_receivables_st: int | None = None
    receivables_allowance_st: int | None = None
    pending_asset_loss: int | None = None
    inventory_section: int | None = None
    inventory: int | None = None
    inventory_allowance: int | None = None
    other_current_assets_section: int | None = None
    prepaid_expenses_st: int | None = None
    vat_credit: int | None = None
    tax_receivables: int | None = None
    repo_gov_bonds_st: int | None = None
    other_current_assets: int | None = None

    _WIGROUP_MAP = {
        **BalanceSheet._WIGROUP_MAP,
        "taisannganhan": "current_assets_section",
        "tienvacackhoantuongduongtien": "cash_and_equivalents_section",
        "tien": "cash",
        "cackhoantuongduongtien": "cash_equivalents",
        "cackhoandaututaichinhnganhan": "short_term_financial_investments_section",
        "chungkhoankinhdoanh": "trading_securities",
        "duphonggiamgiachungkhoankinhdoanh": "trading_securities_allowance",
        "dautunamgiudenngaydaohan_dttcnh": "held_to_maturity_st",
        "cackhoanphaithunganhan": "receivables_short_term_section",
        "phaithukhachhang": "accounts_receivable_st",
        "tratruocchonguoiban": "advances_to_suppliers_st",
        "phaithunoibonganhan": "intercompany_receivables_st",
        "phaithutheotiendokehoachhopdongxaydung": "construction_receivables",
        "phaithuvechovaynganhan": "loans_receivable_st",
        "cackhoanphaithukhac": "other_receivables_st",
        "duphongphaithunganhankhodoi": "receivables_allowance_st",
        "taisanthieuchoxuly": "pending_asset_loss",
        "hangtonkho_tong": "inventory_section",
        "hangtonkho": "inventory",
        "duphonggiamgiahangtonkho": "inventory_allowance",
        "taisannganhankhac_tong": "other_current_assets_section",
        "chiphitratruocnganhan": "prepaid_expenses_st",
        "thuegtgtduockhautru": "vat_credit",
        "thuevacackhoankhacphaithunhanuoc": "tax_receivables",
        "giaodichmuabanlaitraiphieuchinhphu_tsnh": "repo_gov_bonds_st",
        "taisannganhankhac": "other_current_assets",
    }


# NON-CURRENT ASSETS (TÀI SẢN DÀI HẠN)
@dataclass(frozen=True)
class BalanceSheetLongTerm(BalanceSheet):

    non_current_assets_section: int | None = None
    receivables_long_term_section: int | None = None
    accounts_receivable_lt: int | None = None
    advances_to_suppliers_lt: int | None = None
    capital_in_subunits: int | None = None
    intercompany_receivables_lt: int | None = None
    loans_receivable_lt: int | None = None
    other_receivables_lt: int | None = None
    receivables_allowance_lt: int | None = None
    fixed_assets_section: int | None = None
    ppe_tangible: int | None = None
    ppe_tangible_cost: int | None = None
    ppe_tangible_accum_depr: int | None = None
    ppe_finance_leased: int | None = None
    ppe_finance_cost: int | None = None
    ppe_finance_accum_depr: int | None = None
    intangible_assets: int | None = None
    intangible_cost: int | None = None
    intangible_accum_amort: int | None = None
    investment_property: int | None = None
    investment_property_cost: int | None = None
    investment_property_accum_depr: int | None = None
    cip_section: int | None = None
    cip_production: int | None = None
    cip_construction: int | None = None
    investments_long_term_section: int | None = None
    investment_in_subsidiaries: int | None = None
    investment_in_associates: int | None = None
    investment_in_other_entities: int | None = None
    long_term_investment_allowance: int | None = None
    held_to_maturity_lt: int | None = None
    other_non_current_assets_section: int | None = None
    prepaid_expenses_lt: int | None = None
    deferred_tax_assets: int | None = None
    spare_parts_lt: int | None = None
    other_non_current_assets: int | None = None
    goodwill: int | None = None

    _WIGROUP_MAP = {
        **BalanceSheet._WIGROUP_MAP,
        "taisandaihan": "non_current_assets_section",
        "cackhoanphaithudaihan": "receivables_long_term_section",
        "phaithudaihancuakhachhang": "accounts_receivable_lt",
        "tratruocchonguoibandaihan": "advances_to_suppliers_lt",
        "vonkinhdoanhodonvitructhuoc": "capital_in_subunits",
        "phaithunoibodaihan": "intercompany_receivables_lt",
        "phaithuvechovaydaihan": "loans_receivable_lt",
        "phaithudaihankhac": "other_receivables_lt",
        "duphongphaithukhodoi": "receivables_allowance_lt",
        "taisancodinh": "fixed_assets_section",
        "taisancodinhhuuhinh": "ppe_tangible",
        "nguyengia_tscdhh": "ppe_tangible_cost",
        "giatrihaomonluyke_tscdhh": "ppe_tangible_accum_depr",
        "taisancodinhthuetaichinh": "ppe_finance_leased",
        "nguyengia_tscdttc": "ppe_finance_cost",
        "giatrihaomonluyke_tscdttc": "ppe_finance_accum_depr",
        "taisancodinhvohinh": "intangible_assets",
        "nguyengia_tscdvh": "intangible_cost",
        "giatrihaomonluyke_tscdvh": "intangible_accum_amort",
        "batdongsandautu": "investment_property",
        "nguyengia": "investment_property_cost",
        "giatrihaomonluyke": "investment_property_accum_depr",
        "taisandodangdaihan": "cip_section",
        "chiphisanxuatkinhdoanhdodangdaihan": "cip_production",
        "chiphixaydungcobandodang": "cip_construction",
        "daututaichinhdaihan": "investments_long_term_section",
        "dautuvaocongtycon": "investment_in_subsidiaries",
        "dautuvaocongtylienketliendoanh": "investment_in_associates",
        "dautugopvonvaodonvikhac": "investment_in_other_entities",
        "duphonggiamgiadautudaihan": "long_term_investment_allowance",
        "dautunamgiudenngaydaohan_dttcdh": "held_to_maturity_lt",
        "taisandaihankhac_tong": "other_non_current_assets_section",
        "chiphitratruocdaihan": "prepaid_expenses_lt",
        "taisanthuethunhaphoanlai": "deferred_tax_assets",
        "thietbivattuphutungthaythedaihan": "spare_parts_lt",
        "taisandaihankhac": "other_non_current_assets",
        "loithethuongmai": "goodwill",
    }
