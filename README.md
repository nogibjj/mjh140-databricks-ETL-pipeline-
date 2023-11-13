## College Basketball Most Dominant Teams 2002 - 2017

[![CI](https://github.com/nogibjj/mjh140_miniproject6/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/mjh140_miniproject6/actions/workflows/cicd.yml)

### Summary:

The objective of this project is to evaluate college basketball teams on their adjusted offense and adjusted defense scores extracted from the KenPOMS database.

* `Adjusted Offense`: Points Scored per 100 Posessions
* `Adjusted Defense`: Points Allowed per 100 Posessions

Each year, every team in NCAA Division 1 basketball is assigned a cumulative score for both of the adjusted offense and defense metrics. For this analysis, the adjusted defense was subtracted from the adjusted offense to create one "Combined Adjusted O/D" score to represent a teams overall performance on both offense and defense. 

Example: Gonzaga 2002
* `Adjusted Offense`: 115.60
* `Adjusted Defense`: 97.10
* `Combined Adjusted O/D`: 18.50

The database used for this analysis includes all Division 1 teams between 2002 and 2017. The `Combined Adjusted O/D` metric was averaged across all 15 years for each team. The final output is a table of the top 10 teams with the highest `Combined Adjusted O/D`. This table represents the most dominant college basketball teams between 2002 and 2017 with respect to adjusted offense and adjusted defense.

#### Data Analysis:

The data was stored in two tables on a Microsoft Azure Databricks sql database. The two tables were "inner joined" on matching `Team` and `Year`. The new `Combined Adjusted O/D` metric was created as the subtraction of `AdjustedO` and `AdjustedD`. A sql "Group By" command was used to calculate the average `Combined Adjusted O/D` across all teams. See the `averageEFF` function within `/src/main.py` for more details on the SQL commands.

#### Results

The table below represents the 10 teams with the highest average `Combined Adjusted O/D` over the 15 year span from 2002 to 2017:

| Team           | Combined Adjusted O/D |
|----------------|-----------------------|
| Duke           | 26.91                 |
| Kansas         | 26.76                 |
| Wichita St.    | 24.35                 |
| Kentucky       | 24.19                 |
| Tennessee      | 23.70                 |
| Wisconsin      | 22.94                 |
| Florida        | 22.78                 |
| North Carolina | 22.75                 |
| Lousiville     | 22.71                 |
| Oklahoma St    | 22.50                 |

