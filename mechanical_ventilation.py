""" Main script to run experiments over AmsterdamUMCdb

NOTE (09/19/22): Currently aimed at computing table 1 - summary of pCO2,
pO2, sO2 and lactate between arterial and central venous samples within an
hour for every patient not undergoing mechanical ventilation.

Author: Ricardo Kleinlein
Contact: ricardokleinklein@gmail.com
Date: 09/19/2022
"""
import pandas
from database import SQLConnection
from tqdm import tqdm
from typing import Dict
pandas.options.mode.chained_assignment = None  # default='warn'


def update_record(sample: Dict, record: Dict) -> Dict:
    for k in list(sample.keys()):
        record[k].append(sample[k])
    return record


def no_mechvent_admissiontimes(icu: pandas.DataFrame,
                               vent: pandas.DataFrame) -> pandas.DataFrame:
    """ Create a new dataframe that splits admissionid's if the patient
    underwent mechanical ventilation, hence transforming a single instance
    of ICU entrance time - ICU discharge time to
        - ICU entrance time - ICU beggining mechanical ventilation time
        - ICU end mechanical vent. time - ICU discharge time

    Args:
        icu: ICU admissions.
        vent: All admissions that had mechanical ventilation.

    Returns:
        Transformed dataset, extended to provide more detail on mechanical
        ventilation times.

    TODO: Clean code's verbose, refactor functions
    """
    # Straight addition of ICU admissions without mechanical ventilation
    # records
    adms_wo_ventilation = icu[~icu['admissionid'].isin(
        vent['admissionid'].unique())]
    adms_wo_ventilation.loc[:, 'init_unventilated'] = adms_wo_ventilation[
        'admittedat']
    adms_wo_ventilation.loc[:, 'end_unventilated'] = adms_wo_ventilation[
        'dischargedat']

    # Slice admission times when mechanical ventilation is happening
    admissions_with_vent = icu[icu['admissionid'].isin(
        vent['admissionid'].unique())]
    new_records = {k: [] for k in list(adms_wo_ventilation)}
    for index, sample in tqdm(admissions_with_vent.iterrows(), total=len(admissions_with_vent)):
        adm_vent = vent[vent['admissionid'] == sample['admissionid']]

        for j, mech in adm_vent.iterrows():
            had_init = sample['admittedat'] <= mech['start'] < sample['dischargedat']
            had_end = sample['admittedat'] < mech['stop'] <= sample['dischargedat']

            if had_init and had_end:
                pass
                # Ventilation applied during admission --> 2 STAGES
                sample['init_unventilated'] = sample['admittedat']
                sample['end_unventilated'] = mech['start']
                new_records = update_record(sample, new_records)

                sample['init_unventilated'] = mech['stop']
                sample['end_unventilated'] = sample['dischargedat']
                new_records = update_record(sample, new_records)
            elif had_init and not had_end:
                # Ventilation applied and not removed
                sample['init_unventilated'] = sample['admittedat']
                sample['end_unventilated'] = mech['start']
                new_records = update_record(sample, new_records)
            elif not had_init and had_end:
                # Ventilation kept from before and then removed
                sample['init_unventilated'] = mech['stop']
                sample['end_unventilated'] = sample['dischargedat']
                new_records = update_record(sample, new_records)
            elif not had_init and not had_end:
                # Ventilation does not overlap with admission times
                sample['init_unventilated'] = sample['admittedat']
                sample['end_unventilated'] = sample['dischargedat']
                new_records = update_record(sample, new_records)
            else:
                raise RuntimeError('Unexpected scenario - future TODO')

    adms_wo_ventilation = pandas.concat(
        [adms_wo_ventilation, pandas.DataFrame(new_records)]).sort_index()
    return adms_wo_ventilation


def main():
    sql_server = SQLConnection('./amsterdamumcdb.db') # Open local server

    # Tariq's query to retrieve mechanical ventilation
    # TODO: Is this the sort of ventilation we need? Can t know from
    #  the sole terms - colo-stoma item??
    is_ventilation_query = """
    SELECT * FROM processitems
    WHERE itemid IN (
        9328, --Beademen
        10731 --Beademen non-invasief
    )
    """

    # Retrieve admissions and keep only those admitted in ICU
    admissions_query = """SELECT * FROM admissions"""
    admissions = sql_server.execute_read_query(admissions_query)
    admissions_icu = admissions[admissions['location'] == 'IC']

    # Remove admissions whose dischargedat = admittedat - Mistakes in the
    # records?
    admissions_icu = admissions_icu[admissions_icu['dischargedat'] != 0]

    # Retrieve admissions in which mechanical ventilation happened
    ventilation = sql_server.execute_read_query(is_ventilation_query)
    ventilation = ventilation[ventilation['item'] == 'Beademen']

    # Remove those admissions intervals during which mechanical
    # ventilation happened
    icu_not_mechvent = no_mechvent_admissiontimes(admissions_icu, ventilation)

    # Add to local server
    sql_server.save_to_database('icu_wo_mechvent', icu_not_mechvent)

    # Test some instances
    # ad = 4630
    # instance_icu = admissions_icu[admissions_icu['admissionid'] == ad]
    # print('ICU Admission')
    # print(instance_icu.iloc[0])
    # instance_vent = ventilation[ventilation['admissionid'] == ad]
    # print('Ventilation times')
    # print(instance_vent.iloc[0])
    # instance_final = icu_not_mechvent[icu_not_mechvent['admissionid'] == ad]
    # print('Final admission times')
    # print(instance_final)

    sql_server.close()  # Close local server


if __name__ == "__main__":
    main()
