from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime, timedelta

def _parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        except Exception:
            return None

def _normalize_field(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v == "" or v.lower() in ("null", "none"):
            return None
        return v
    return str(value)

def build_query(parsed: Dict[str, Any]) -> Tuple[Optional[str], List[Any]]:
    intent = _normalize_field(parsed.get("intent"))
    target = _normalize_field(parsed.get("target"))
    col = _normalize_field(parsed.get("column"))
    agg = _normalize_field(parsed.get("aggregate")) or "count"
    filters = parsed.get("filters") or {}

    f_creator = _normalize_field(filters.get("creator_id"))
    f_date_from = _normalize_field(filters.get("date_from"))
    f_date_to = _normalize_field(filters.get("date_to"))
    f_date_field = _normalize_field(filters.get("date_field"))
    f_comparison = _normalize_field(filters.get("comparison"))
    f_period_hours = filters.get("period_hours")
    f_period_anchor = _normalize_field(filters.get("period_anchor"))

    if not target or not intent:
        return None, []

    where_clauses: List[str] = []
    params: List[Any] = []
    param_idx = 1

    needs_video_join = False
    if target == "video_snapshots":
        if f_period_hours is not None or f_creator is not None:
            needs_video_join = True

    def snap(colname: str) -> str:
        return f"vs.{colname}" if target == "video_snapshots" else colname

    def video(colname: str) -> str:
        if target == "video_snapshots" and needs_video_join:
            return f"v.{colname}"
        return colname

    if not col:
        if intent in ("count", "count_distinct"):
            if target == "videos":
                col = "id"
            elif target == "video_snapshots":
                col = "video_id" if intent == "count_distinct" else "id"
            else:
                col = "id"
        elif intent in ("sum", "sum_delta"):
            return None, []
        else:
            return None, []

    def col_ref(column_name: str) -> str:
        if target == "video_snapshots":
            return f"vs.{column_name}"
        return column_name

    if f_creator:
        if target == "videos":
            where_clauses.append(f"creator_id = ${param_idx}")
        else:
            if needs_video_join:
                where_clauses.append(f"{video('creator_id')} = ${param_idx}")
            else:
                needs_video_join = True
                where_clauses.append(f"{video('creator_id')} = ${param_idx}")
        params.append(f_creator)
        param_idx += 1

    date_field_name = None
    if f_date_from or f_date_to:
        if f_date_field == "video_created_at":
            date_field_name = video("video_created_at")
        elif f_date_field == "created_at":
            if target == "video_snapshots":
                date_field_name = snap("created_at")
            else:
                date_field_name = "created_at"
        else:
            if target == "videos":
                date_field_name = video("video_created_at")
            elif target == "video_snapshots":
                date_field_name = snap("created_at")
            else:
                date_field_name = "created_at"

        if f_date_from and f_date_to:
            start = _parse_iso_datetime(f_date_from)
            end = _parse_iso_datetime(f_date_to)
            if start is not None and end is not None:
                end = end + timedelta(days=1)
                where_clauses.append(f"{date_field_name} >= ${param_idx} AND {date_field_name} < ${param_idx+1}")
                params.append(start)
                params.append(end)
                param_idx += 2
        elif f_date_from:
            start = _parse_iso_datetime(f_date_from)
            if start is not None:
                where_clauses.append(f"{date_field_name} >= ${param_idx}")
                params.append(start)
                param_idx += 1
        elif f_date_to:
            end = _parse_iso_datetime(f_date_to)
            if end is not None:
                end = end + timedelta(days=1)
                where_clauses.append(f"{date_field_name} < ${param_idx}")
                params.append(end)
                param_idx += 1

    if f_comparison:
        import re
        m = re.match(r'^\s*([a-z_]+)?\s*(>=|<=|=|>|<)\s*(\d+)\s*$', f_comparison)
        if m:
            comp_col = m.group(1) or col
            op = m.group(2)
            val = int(m.group(3))

            if target == "video_snapshots" and comp_col in ("views_count","likes_count","comments_count","reports_count",
                                                           "delta_views_count","delta_likes_count","delta_comments_count","delta_reports_count","video_id","id"):
                prefixed = snap(comp_col)
            elif comp_col == "video_created_at" and needs_video_join:
                prefixed = video("video_created_at")
            else:
                if comp_col in ("creator_id", "video_created_at") and needs_video_join:
                    prefixed = video(comp_col)
                else:
                    prefixed = comp_col
            where_clauses.append(f"{prefixed} {op} ${param_idx}")
            params.append(val)
            param_idx += 1

    period_hours = None
    if f_period_hours is not None:
        try:
            period_hours = int(f_period_hours)
        except Exception:
            period_hours = None

    period_anchor = f_period_anchor

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    sql: Optional[str] = None

    if intent == "count":
        if col == "id" and target == "videos":
            sql = f"SELECT COUNT(*)::bigint FROM videos WHERE {where_sql};"
        else:
            if target == "video_snapshots":
                colname = snap(col)
            else:
                colname = col
            sql = f"SELECT COUNT({colname})::bigint FROM {target} {'vs' if target=='video_snapshots' else ''} WHERE {where_sql};"

    elif intent == "count_distinct":
        if target == "video_snapshots":
            colname = snap(col)
            sql = f"SELECT COUNT(DISTINCT {colname})::bigint FROM video_snapshots vs WHERE {where_sql};"
        else:
            colname = col
            sql = f"SELECT COUNT(DISTINCT {colname})::bigint FROM {target} WHERE {where_sql};"

    elif intent in ("sum", "sum_delta"):
        if target == "video_snapshots" and needs_video_join:
            if period_hours is not None and period_anchor == "video_created_at":
                params.append(period_hours)
                period_param_idx = param_idx
                param_idx += 1

                sql = (
                    "SELECT COALESCE(SUM(vs.{col}),0)::bigint "
                    "FROM video_snapshots vs "
                    "JOIN videos v ON v.id = vs.video_id "
                    "WHERE vs.created_at >= v.video_created_at "
                    f"AND vs.created_at < v.video_created_at + make_interval(hours => ${period_param_idx}) "
                ).format(col=col)
                if where_sql and where_sql != "TRUE":
                    sql += f" AND ({where_sql})"
                sql += ";"
            else:
                sql = f"SELECT COALESCE(SUM(vs.{col}),0)::bigint FROM video_snapshots vs JOIN videos v ON v.id = vs.video_id WHERE {where_sql};"
        else:
            if target == "video_snapshots":
                sql = f"SELECT COALESCE(SUM(vs.{col}),0)::bigint FROM video_snapshots vs WHERE {where_sql};"
            else:
                sql = f"SELECT COALESCE(SUM({col}),0)::bigint FROM {target} WHERE {where_sql};"
    else:
        sql = None

    return sql, params