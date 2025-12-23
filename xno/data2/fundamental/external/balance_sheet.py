# import logging

# from sqlalchemy import text

# from xno.connectors.sql import SqlSession
# from xno.data2.fundamental.external.base import WiGroupBaseAPI

# logger = logging.getLogger(__name__)


# class WiGroupBalanceSheetAPI(WiGroupBaseAPI):
#     def get_balance_sheet(self, mack: str) -> dict:
#         if __debug__:
#             logger.debug("Fetching balance sheet info for code: %s", mack)

#         with SqlSession(self._database_name) as session:
#             sql = """
#                   SELECT
#                     code, quy, nam, doanhthubanhangvacungcapdichvu, cackhoangiamtrudoanhthu
#                   FROM wigroup_api.Tai_chinh_doanh_nghiep_Can_doi_ke_toan
#                   WHERE
#                     mack = :mack
#                   """
#             result = session.execute(
#                 text(sql),
#                 dict(mack=mack),
#             )
#             rows = result.fetchall()

#             if __debug__:
#                 logger.debug("Retrieved %d rows for code: %s", len(rows), mack)
#         return rows
