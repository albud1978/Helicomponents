query = f"""
    WITH
        (
            SELECT
                mi8t_count,
                mi17_count
            FROM OlapCube_VNV
            WHERE Dates = '{dates[0].strftime('%Y-%m-%d')}'
            LIMIT 1
        ) AS counts
    SELECT
        serialno,
        Dates,
        Status,
        Status_P,
        ...
    FROM OlapCube_VNV
    WHERE Dates IN ({', '.join([f"'{date.strftime('%Y-%m-%d')}'" for date in dates])})
    ORDER BY Dates, serialno
"""