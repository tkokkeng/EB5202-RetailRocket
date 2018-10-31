######################################################################################################################
# Import libraries
######################################################################################################################
import pandas as pd
import datetime

######################################################################################################################
# Private Parameters
######################################################################################################################


######################################################################################################################
# Public Functions
######################################################################################################################
# This function returns zzz.
def sessionize(events_df: pd.DataFrame):

    session_duration = datetime.timedelta(minutes=30)
    gpby_visitorid = events_df.groupby('visitorid')

    session_list = []
    for a_visitorid in gpby_visitorid.groups:

        visitor_df = events_df.loc[gpby_visitorid.groups[a_visitorid], :].sort_values('date')
        if not visitor_df.empty:
            visitor_df.sort_values('date', inplace=True)

            # Initialise first session
            startdate = visitor_df.iloc[0, :]['date']
            visitorid = a_visitorid
            items_dict = dict([ (i, []) for i in events_df['event_type'].cat.categories ])
            for index, row in visitor_df.iterrows():

                # Check if current event date is within session duration
                if row['date'] - startdate <= session_duration:
                # Add itemid to the list according to event type (i.e. view, addtocart or transaction)
                    items_dict[row['event']].append(row['itemid'])
                    enddate = row['date']
                else:
                    # Complete current session
                    session_list.append([visitorid, startdate, enddate] + [ value for key, value in items_dict.items() ])
                    # Start a new session
                    startdate = row['date']
                    items_dict = dict([ (i, []) for i in events_df['event_type'].cat.categories ])
                    # Add current itemid
                    items_dict[row['event']].append(row['itemid'])

            # If dict if not empty, add item data as last session.
            incomplete_session = False
            for key, value in items_dict.items():
                if value:
                    incomplete_session = True
                    break
            if incomplete_session:
                session_list.append([visitorid, startdate, enddate] + [value for key, value in items_dict.items()])

    return session_list


######################################################################################################################
# Private Functions
######################################################################################################################
