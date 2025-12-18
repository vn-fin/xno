def document(*, group: str, name: str, prototype: str | None = None, docs: str | None = None, access: list[str] | None = None):
    def decorator(obj):
        doc = {
            "group": group,
            "name": name,
            "docs": docs or (getattr(obj, "__doc__", "") or ""),
            "access": access,
        }
        if prototype is not None:
            doc["prototype"] = prototype

        if isinstance(obj, property):
            if obj.fget:
                obj.fget.__doc_meta__ = doc
            return obj

        obj.__doc_meta__ = doc
        return obj

    return decorator
