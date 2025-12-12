import logging
from dataclasses import MISSING, fields

from xno.utils.struct import DefaultStruct, parse_field


class BaseEntity(DefaultStruct):

    @classmethod
    def from_json(cls, raw: object):
        parsed = {}
        try:
            for f in fields(cls):
                parsed[f.name] = parse_field(raw.get(f.name, MISSING), f.type)
        except Exception as e:
            logging.exception(f"Error parsing {raw}. Result default None", exc_info=e)
            return None

        return cls(**parsed)
