"""
Databricks query script
"""
import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, parent_dir)

from mylib.sqlconn import sqlConnect, sqlClose

def averageEFF(c):
    '''Calculate average combined adjusted offense/defense by team between 2002 and 2018.
       Return top 10 teams and their respective scores.'''

    query = '''
        WITH t AS (
        SELECT t1.Year, t1.Team, t1.AdjustD, t2.AdjustO, (t2.AdjustO - t1.AdjustD) 
        AS Largest_OD_Diff 
        FROM kenpom_data AS t1
        INNER JOIN kenpoms_adj_o AS t2 
        ON t1.Team = t2.Team 
        AND t1.Year = t2.Year
        )

        SELECT t.Team, AVG(t.Largest_OD_Diff) AS Average_EFF
        FROM t
        GROUP BY t.Team
        ORDER BY Average_EFF DESC'''
    
    c.execute(query)
    results = c.fetchall()
    top10 = {results[i][0]: results[i][1] for i in range(10)}
    col_names = ["Team", "Combined Adjusted O/D"]
    top10_df = pd.DataFrame(list(top10.items()), columns= col_names)
    print(top10_df)


if __name__ == "__main__":
      print("Connecting to Azure Databricks - Kenpoms Database")
      cursor, status = sqlConnect()

      if status != "Success":
           print('Error Occurred. Check error_log.txt.')
           sys.exit()

      averageEFF(cursor)

      print("Closing connection to database.")
      sqlClose(cursor)