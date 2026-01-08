
import blake3
import pandas as pd

from typing import Union, Callable, Any, List, Optional

def hash_columns(
    *cols: Any,
    hasher_factory: Callable[[], Any] = blake3.blake3,
    sep: str = "|"
) -> List[Optional[str]]:
    """
    Generates a hash per row by combining the values of the provided columns.
    A row with only empty values (None, NaN, '') will result in None.
    :param cols: pandas.Series or polars.Series
    :param hasher_factory: hash object factory (e.g., blake3.blake3)
    :param sep: separator used to concatenate the values of each row
    :return: list of strings with the hexadecimal digest of each row, or None for empty rows
    """
    if not cols:
        return []

    lists = [col.to_list() for col in cols]
    n = len(lists[0])
    if any(len(lst) != n for lst in lists):
        raise ValueError("As colunas devem ter o mesmo tamanho")

    hashes: List[Optional[str]] = []
    for row in zip(*lists):
        clean_vals = []
        for v in row:
            if v is None or pd.isna(v):
                clean_vals.append("")
            else:
                clean_vals.append(str(v))

        if all(s == "" for s in clean_vals):
            hashes.append(None)
        else:
            data = sep.join(clean_vals).encode("utf-8")
            h = hasher_factory()
            h.update(data)
            hashes.append(h.hexdigest())        
    return hashes