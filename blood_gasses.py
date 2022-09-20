"""

Author: Ricardo Kleinlein
Contact: ricardokleinklein@gmail.com
Date: 09/19/22
"""
from database import SQLConnection


def main():
    sql_server = SQLConnection('./amsterdamumcdb.db')  # Open local server

    adms_not_ventilated_query = "SELECT admissionid from icu_wo_mechvent"
    adms = sql_server.execute_read_query(adms_not_ventilated_query)
    adms = adms['admissionid'].unique()

    top = adms[:3]
    top = ', '.join([str(s) for s in top])
    query = f"""
    SELECT * from numericitems num
    WHERE 
    (admissionid in ({top}))
    AND
    (num.fluidout IS NOT NULL)
    ORDER BY admissionid"""
    example = sql_server.execute_read_query(query)
    print(example)


    sql_server.close()  # Close local server


if __name__ == "__main__":
    main()
