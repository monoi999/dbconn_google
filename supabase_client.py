from typing import Optional, Dict, Any, List
from sqlalchemy import text
import pandas as pd
import os

DB_TABLE = "score_data"


def _read_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df


def get_by_id(engine, id: str, csv_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if engine is not None:
        with engine.connect() as conn:
            q = text(f"SELECT * FROM {DB_TABLE} WHERE id = :id LIMIT 1")
            res = conn.execute(q, {"id": id}).mappings().fetchone()
            return dict(res) if res is not None else None
    # CSV fallback
    if csv_path and os.path.exists(csv_path):
        df = _read_csv(csv_path)
        mask = df['id'].astype(str) == str(id)
        if mask.any():
            return df.loc[mask].iloc[0].to_dict()
    return None


def get_all(engine, csv_path: Optional[str] = None) -> List[Dict[str, Any]]:
    if engine is not None:
        with engine.connect() as conn:
            q = text(f"SELECT * FROM {DB_TABLE} ORDER BY id")
            res = conn.execute(q).mappings().fetchall()
            return [dict(r) for r in res]
    if csv_path and os.path.exists(csv_path):
        df = _read_csv(csv_path)
        return df.to_dict(orient="records")
    return []


def create_record(engine, rec: Dict[str, Any], csv_path: Optional[str] = None) -> Dict[str, Any]:
    # rec must contain 'id' key
    if 'id' not in rec or not str(rec['id']).strip():
        raise ValueError("Record must include non-empty 'id'")

    rid = str(rec['id'])
    if engine is not None:
        # check exists
        with engine.connect() as conn:
            exists_q = text(f"SELECT 1 FROM {DB_TABLE} WHERE id = :id LIMIT 1")
            found = conn.execute(exists_q, {"id": rid}).fetchone()
            if found:
                raise ValueError("Record with given id already exists")
            # build insert
            cols = []
            vals = []
            params = {}
            for k, v in rec.items():
                cols.append(k)
                vals.append(f":{k}")
                params[k] = v
            insert_sql = f"INSERT INTO {DB_TABLE} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            conn.execute(text(insert_sql), params)
            return rec
    # CSV fallback: append if id not present
    if csv_path and os.path.exists(csv_path):
        df = _read_csv(csv_path)
        if 'id' in df.columns and (df['id'].astype(str) == rid).any():
            raise ValueError("Record with given id already exists (csv)")
        df = df.append(rec, ignore_index=True)
        df.to_csv(csv_path, index=False)
        return rec
    raise RuntimeError("No data engine available and no csv_path provided")


def update_record(engine, id: str, rec: Dict[str, Any], version: Optional[int] = None, csv_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    # rec contains fields to set (excluding id)
    if engine is not None:
        with engine.begin() as conn:
            params = {**rec}
            params['id'] = id
            if version is not None:
                # optimistic lock: require version match
                # attempt to update version and fields
                set_parts = []
                for k in rec.keys():
                    set_parts.append(f"{k} = :{k}")
                # update version and updated_at if columns exist in DB schema; use safe SQL that will error if not present
                set_parts.append("version = version + 1")
                set_sql = ", ".join(set_parts)
                update_sql = f"UPDATE {DB_TABLE} SET {set_sql}, updated_at = now() WHERE id = :id AND version = :version"
                params['version'] = version
                res = conn.execute(text(update_sql), params)
                if res.rowcount == 0:
                    return None
                # return updated record
                r = conn.execute(text(f"SELECT * FROM {DB_TABLE} WHERE id = :id"), {"id": id}).mappings().fetchone()
                return dict(r) if r is not None else None
            else:
                # plain update without optimistic lock
                set_parts = []
                for k in rec.keys():
                    set_parts.append(f"{k} = :{k}")
                set_sql = ", ".join(set_parts)
                update_sql = f"UPDATE {DB_TABLE} SET {set_sql} WHERE id = :id"
                res = conn.execute(text(update_sql), params)
                if res.rowcount == 0:
                    return None
                r = conn.execute(text(f"SELECT * FROM {DB_TABLE} WHERE id = :id"), {"id": id}).mappings().fetchone()
                return dict(r) if r is not None else None
    # CSV fallback
    if csv_path and os.path.exists(csv_path):
        df = _read_csv(csv_path)
        mask = df['id'].astype(str) == str(id)
        if not mask.any():
            return None
        for k, v in rec.items():
            if k in df.columns:
                df.loc[mask, k] = v
            else:
                df[k] = None
                df.loc[mask, k] = v
        df.to_csv(csv_path, index=False)
        return df.loc[mask].iloc[0].to_dict()
    return None


def delete_record(engine, id: str, csv_path: Optional[str] = None) -> bool:
    if engine is not None:
        with engine.begin() as conn:
            res = conn.execute(text(f"DELETE FROM {DB_TABLE} WHERE id = :id"), {"id": id})
            return res.rowcount > 0
    if csv_path and os.path.exists(csv_path):
        df = _read_csv(csv_path)
        mask = df['id'].astype(str) == str(id)
        if not mask.any():
            return False
        df = df.loc[~mask]
        df.to_csv(csv_path, index=False)
        return True
    return False
