import inspect


def get_documents(*objects: list[object]) -> list[dict]:
    docs_list = []
    for o in objects:
        docs_list.extend(_get_documents(o))
    return docs_list

def _get_documents(obj: object) -> list[dict]:
    docs_list = []

    # Methods
    for _, member in inspect.getmembers(obj, inspect.ismethod):
        meta = getattr(member, "__doc_meta__", None)
        if meta:
            sig = inspect.signature(member)
            docs_list.append({
                "group": meta["group"],
                "name": meta["name"],
                "prototype": meta.get("prototype", f"{member.__name__}{sig}"),
                "docs": meta["docs"],
                "access": meta["access"],
            })

    # Properties
    for _, member in inspect.getmembers(type(obj), lambda x: isinstance(x, property)):
        if member.fget:
            meta = getattr(member.fget, "__doc_meta__", None)
            if meta:
                # Build a "prototype" string with no 'self'
                docs_list.append({
                    "group": meta["group"],
                    "name": meta["name"],
                    "prototype": meta.get("prototype", f"{member.fget.__name__}"),
                    "docs": meta["docs"],
                    "access": meta["access"],
                })

    return docs_list