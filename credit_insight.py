import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from typing import Union, List


class FeatureInsight:
    ##############################################  DISTRIBUTION  ##############################################
    def distribution(self, df: pd.DataFrame,
                            groupby_col: Union[str, List[str]],
                            agg_col: Union[str, List[str]],
                            agg_func: str = 'sum') -> pd.DataFrame:
        """
        Calculate the distribution of a metric for each group in the DataFrame.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing data.
        - groupby_col: Union[str, List[str]]
            The column used for grouping the data.
        - agg_col: Union[str, List[str]]
            The column needs to be aggregated.
        - agg_func: str
            The function for aggregation: 'sum', 'mean', 'count', etc.

        Returns:
        - pd.DataFrame
            DataFrame with the distribution of the specified metric for each group.
        """
        # Group by groupby_col and sum agg_col
        dist = df.groupby(groupby_col)[agg_col].agg(agg_func).reset_index()

        # Calculate distribution
        if isinstance(agg_col, list):
            for col in agg_col:
                dist['%' + col] = dist[col] / dist[col].sum()
        else:
            dist['%' + agg_col] = dist[agg_col] / dist[agg_col].sum()

        return dist.sort_values(groupby_col, ascending = True)


    def distribution_over_time(self, df: pd.DataFrame,
                                        groupby_col: Union[str, List[str]],
                                        agg_col: Union[str, List[str]],
                                        time_col: str,
                                        time_range: List[str] = None,
                                        agg_func: str = 'sum') -> pd.DataFrame:
        """
        Calculate distribution of a metric over time for each group in the DataFrame.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing data.
        - groupby_col: Union[str, List[str]]
            The column used for grouping the data.
        - agg_col: Union[str, List[str]]
            The column that needs to be aggregated.
        - time_col: str
            Field name of the time column.
        - time_range: List[str], optional
            The range of time. If not provided, it will include all values in time_col.
        - agg_func: str, optional
            The function for aggregation: 'sum', 'mean', 'count', etc.

        Returns:
        - pd.DataFrame
            DataFrame with the distribution of the specified metric for each group over time.
        """
        # Set default time_range if not provided
        time_range = time_range or sorted(df[time_col].unique())

        # Filter time range
        time_filter = df[df[time_col].isin(time_range)]

        # Initialize null Dataframe
        result = pd.DataFrame()

        # Calculate distribution over time
        for time in time_range:

            # Calculate distribution each time bucket
            dist = self.distribution(time_filter[time_filter[time_col] == time], groupby_col, agg_col, agg_func)

            # Insert the column TIME
            _ = pd.concat([pd.Series(time, name = time_col), dist], axis = 1)
            _[time_col].fillna(time, inplace = True)

            # UNION the null Dataframe
            result = pd.concat([result, _], axis = 0)

        result.sort_values([time_col, groupby_col], ascending = True, inplace = True)

        return result


    ############################################## DEFAULT RATE ##############################################

    def default_rate(self, 
                    df: pd.DataFrame,
                    bin_col: Union[str, List[str]],
                    bad_col: str,
                    population_col: str) -> pd.DataFrame:
        """
        Calculate the default rate for each group in the DataFrame.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing loan data.
        - bin_col: Union[str, List[str]]
            The column used for grouping the data.
        - bad_col: str
            The column representing default or bad loans.
        - population_col: str
            The column representing the total number of loans or total applications.

        Returns:
        - pd.DataFrame
            DataFrame with default rate calculated for each group, sorted by the group column.
        """
        # Group the DataFrame by groupby_col and aggregate the sum of population_col and bad_col
        grouped_df = df.groupby(bin_col)[[population_col, bad_col]].agg(sum).reset_index()

        # Calculate the default rate and add a new column '%DEFAULT'
        grouped_df['%DEFAULT'] = grouped_df[bad_col] / grouped_df[population_col]

        # Sort groupby_col in ascending order
        result_df = grouped_df.sort_values(bin_col, ascending=True)

        return result_df

    def default_rate_over_time(self, df: pd.DataFrame,
                                bin_col: Union[str, List[str]],
                                bad_col: str,
                                population_col: str,
                                time_col: str,
                                time_range: List[str] = None):
        """
            Calculate default rate over time for each group in the DataFrame.

            Parameters:
            - df: pd.DataFrame
                The input DataFrame containing loan data.
            - bin_col: Union[str, List[str]]
                The column used for grouping the data.
            - bad_col: str
                The column representing default or bad loans.
            - population_col: str
                The column representing the total number of loans or total applications.
            - time_col: str
                The field name of the time column.
            - time_range: List[str], optional
                The range of time. If not provided, it will be extracted from the unique values in the time column.

            Returns:
            - pd.DataFrame
                DataFrame with default rate calculated for each group over time.
        """
        # Set default time_range if not provided
        time_range = time_range or sorted(df[time_col].unique())

        # Filter time range
        time_filter = df[df[time_col].isin(time_range)]

        # Initialize null Dataframe
        result = pd.DataFrame()

        # Calculate default rate over time
        for time in time_range:

            # Calculate default rate each time bucket
            default_rate_ = self.default_rate(time_filter[time_filter[time_col] == time], bin_col, bad_col, population_col)

            # Insert column TIME
            _ = pd.concat([pd.Series(time, name = time_col), default_rate_], axis = 1)
            _[time_col].fillna(time, inplace = True)

            # Union the null DataFrame
            result = pd.concat([result, _], axis = 0)

        # Sort column TIME in ascending order
        result.sort_values([time_col, bin_col], ascending = True, inplace = True)

        return result

    ############################################## Weight of Evidence + Information Value ##############################################

    def woe_iv(self, df, bin_col, bad_col, population_col):
        df_grouped = df.groupby(bin_col)[[bad_col, population_col]].agg(sum).reset_index()
        df_grouped.rename(columns = {bad_col : 'EVENT', population_col : 'TOTAL'}, inplace = True)
        df_grouped['NONEVENT'] = df_grouped['TOTAL'] - df_grouped['EVENT']
        
        event_dist = self.distribution(df_grouped[[bin_col, 'EVENT']], bin_col, 'EVENT')[[bin_col, '%EVENT']]
        nonevent_dist = self.distribution(df_grouped[[bin_col, 'NONEVENT']], bin_col, 'NONEVENT')[[bin_col, '%NONEVENT']]

        woe_iv_table = pd.merge(event_dist, nonevent_dist, on = bin_col)
        
        woe_iv_table['WOE'] = np.log(woe_iv_table['%NONEVENT'] / woe_iv_table['%EVENT'])
        woe_iv_table['IV'] = (woe_iv_table['%NONEVENT'] - woe_iv_table['%EVENT']) *  woe_iv_table['WOE']
        
        return woe_iv_table


    def woe_iv_over_time(self, df, bin_col, bad_col, population_col, time_col, time_range=None, show_total=False):
        # Set default time_range if not provided
        time_range = time_range or sorted(df[time_col].unique())

        # Filter time range
        time_filter = df[df[time_col].isin(time_range)]

        # Initialize null Dataframe
        result = pd.DataFrame()

        # Calculate default rate over time
        for time in time_range:

            # Calculate default rate each time bucket
            woe_iv_ = self.woe_iv(time_filter[time_filter[time_col] == time], bin_col, bad_col, population_col)

            # Insert column TIME
            _ = pd.concat([pd.Series(time, name = time_col), woe_iv_], axis = 1)
            _[time_col].fillna(time, inplace = True)

            # Union the null DataFrame
            result = pd.concat([result, _], axis = 0)

        # Sort column TIME in ascending order
        result.sort_values([time_col, bin_col], ascending = True, inplace = True)

        if show_total == True :
            return result.groupby([time_col])['IV'].agg(sum).reset_index()
        else:
            return result[[time_col, bin_col, 'WOE', 'IV']]


    def psi(self, df, bin_col, time_col, app_col, sel_time=None, dev_time=None):
        
        dev_time = dev_time or sorted(df[time_col].unique())[0]
        sel_time = sel_time or sorted(df[time_col].unique())[-1]
        
        dev_dist = self.distribution(df[df[time_col] == dev_time], groupby_col=bin_col, agg_col=app_col) # agg_func should be 'count'
        sel_dist = self.distribution(df[df[time_col] == sel_time], groupby_col=bin_col, agg_col=app_col) # agg_func should be 'count'
        
        # Calculate PSI
        psi_cal = pd.merge(sel_dist, dev_dist, how='left', on=bin_col)
        psi_cal[f'%{app_col}_x - %{app_col}_y'] = psi_cal[f'%{app_col}_x'] - psi_cal[f'%{app_col}_y']
        psi_cal[f'ln(%{app_col}_x - %{app_col}_y)'] = np.log(psi_cal[f'%{app_col}_x'] / psi_cal[f'%{app_col}_y'])
        psi_cal['PSI'] = psi_cal[f'ln(%{app_col}_x - %{app_col}_y)'] * psi_cal[f'%{app_col}_x - %{app_col}_y']

        return psi_cal


    def psi_over_time(self, df, bin_col, time_col, app_col, sel_time = None, dev_time=None):
        
        dev_time = dev_time or sorted(df[time_col].unique())[0]
        sel_time = sel_time or sorted(df[time_col].unique())[1:]

        # Initialize a null DataFrame
        result = pd.DataFrame()

        for time in sel_time:
            psi = self.psi(df=df,
                                bin_col=bin_col,
                                time_col=time_col,
                                app_col=app_col,
                                dev_time=dev_time,
                                sel_time=time)

            psi['TIME'] = time  # Add the 'TIME' column to the DataFrame psi
            result = pd.concat([result, psi], axis=0)

        # Sort the DataFrame by time
        result.sort_values('TIME', ascending=True, inplace=True)

        return result

class ModelInsight:
    ##############################################  GINI  ##############################################
    fi = FeatureInsight()
    def calculate_gini(self, df: pd.DataFrame,
                    score_col: str,
                    bad_col: str,
                    population_col: str) -> float:
        """
        Calculate the Gini coefficient for a given DataFrame and columns.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing loan data.
        - score_col: str
            The column used for grouping or scoring the data.
        - bad_col: str
            The column representing default or bad loans.
        - population_col: str
            The column representing the total number of loans or total applications.

        Returns:
        - float
            Gini coefficient value.
        """

        # Group by score_col and aggregate the sum of bad_col and population_col
        grouped_df = df.groupby(score_col)[[bad_col, population_col]].agg(sum).reset_index()

        # Calculate non-default
        grouped_df['NON_DEFAULT'] = grouped_df[population_col] - grouped_df[bad_col]
        grouped_df.rename(columns={bad_col: 'DEFAULT'}, inplace=True)

        # Calculate percentage distribution
        dist_bad = self.distribution(df=grouped_df, groupby_col=score_col, agg_col=['DEFAULT'])
        dist_good = self.distribution(df=grouped_df, groupby_col=score_col, agg_col=['NON_DEFAULT'])

        # Sort percentage distribution by ascending order
        dist_bad.sort_values('%DEFAULT', ascending=True, inplace=True)

        # Merge the bad and good distributions
        gini_cal = pd.merge(dist_bad, dist_good, how='left', on=score_col)

        # Calculate cumulative percentages and Area Under The Curve
        gini_cal['%DEFAULT_cum'] = gini_cal['%DEFAULT'].cumsum()
        gini_cal['%NON_DEFAULT_cum'] = gini_cal['%NON_DEFAULT'].cumsum()
        gini_cal['AUC'] = (
            (gini_cal['%DEFAULT_cum'] + gini_cal['%DEFAULT_cum'].shift(1)) *
            (gini_cal['%NON_DEFAULT_cum'] - gini_cal['%NON_DEFAULT_cum'].shift(1)) / 2
        )

        # Calculate Gini coefficient
        gini_coef = 1 - 2 * gini_cal['AUC'].sum()

        return gini_coef



    def calculate_gini_over_time(self, df: pd.DataFrame,
                                score_col: str,
                                bad_col: str,
                                population_col: str,
                                time_col: str,
                                time_range: List[str] =None) -> pd.DataFrame:
        """
        Calculate Gini coefficient over time for each group in the DataFrame.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing loan data.
        - score_col: str
            The column used for grouping or deciling the data.
        - bad_col: str
            The column representing default or bad loans.
        - population_col: str
            The column representing the total number of loans or total applications.
        - time_col: str
            Field name of the time column.
        - time_range: List[str], optional
            The range of time. If not provided, it will include all values in time_col.

        Returns:
        - pd.DataFrame
            DataFrame with Gini coefficient calculated for each group over time.
        """
        # Set default time_range if not provided
        time_range = time_range or sorted(df[time_col].unique())

        # Initialize an empty list
        result = list()

        # Filter the input time
        time_filter = df[df[time_col].isin(time_range)]

        # Calculate GINI each time bucket
        for time in time_range:
            gini = ModelInsight.calculate_gini(time_filter[time_filter[time_col] == time], score_col, bad_col, population_col)
            result.append([time, gini])

        # Convert the list of results to a DataFrame
        result_df = pd.DataFrame(result, columns=['TIME', 'GINI']).sort_values('TIME', ascending=True)

        return result_df


    ##############################################  PSI  ##############################################

    def calculate_psi(self, df: pd.DataFrame,
                    score_col: str,
                    time_col: str,
                    app_col: str,
                    dev_time: str,
                    sel_time: str) -> float:
        """
        Calculate the Population Stability Index (PSI) for a specified score-range over two time periods.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing data.
        - score_col: str
            The field name of the score-range for PSI calculation.
        - time_col: str
            Field name of the time column.
        - app_col: str
            Field name of the application column.
        - dev_time: str
            Developed time period.
        - sel_time: str
            Selected time period.

        Returns:
        - float
            PSI value.
        """
        # Calculate distribution of developed and selected time window
        dev_dist = ModelInsight.fi.distribution(df[df[time_col] == dev_time], groupby_col=score_col, agg_col=app_col) # agg_func should be 'count'
        sel_dist = ModelInsight.fi.distribution(df[df[time_col] == sel_time], groupby_col=score_col, agg_col=app_col) # agg_func should be 'count'

        # Calculate PSI
        psi_cal = pd.merge(sel_dist, dev_dist, how='left', on=score_col)
        psi_cal[f'%{app_col}_x - %{app_col}_y'] = psi_cal[f'%{app_col}_x'] - psi_cal[f'%{app_col}_y']
        psi_cal[f'ln(%{app_col}_x - %{app_col}_y)'] = np.log(psi_cal[f'%{app_col}_x'] / psi_cal[f'%{app_col}_y'])
        psi_cal['PSI'] = psi_cal[f'ln(%{app_col}_x - %{app_col}_y)'] * psi_cal[f'%{app_col}_x - %{app_col}_y']

        return psi_cal['PSI'].sum()

    def calculate_psi_over_time(self, df: pd.DataFrame,
                                score_col: str,
                                app_col: str,
                                time_col: str,
                                dev_time: str,
                                sel_time: List[str]) -> pd.DataFrame:
        """
        Calculate Population Stability Index (PSI) over time for a specified score-range.

        Parameters:
        - df: pd.DataFrame
            The input DataFrame containing data.
        - score_col: str
            The field name of the score-range for PSI calculation.
        - time_col: str
            Field name of the time column.
        - app_col: str
            Field name of the application column.
        - dev_time: str
            Developed time period.
        - sel_time: list
            List of selected time periods.

        Returns:
        - pd.DataFrame
            DataFrame with PSI values for each time period.
        """
        # Initialize a null DataFrame
        result = pd.DataFrame()

        # Calculate PSI for each time bucket
        for time in sel_time:
            psi = ModelInsight.calculate_psi(df=df,
                                score_col=score_col,
                                time_col=time_col,
                                app_col=app_col,
                                dev_time=dev_time,
                                sel_time=time)

            # Concatenate the results to the null DataFrame
            _ = pd.concat([pd.Series(time, name='TIME'), pd.Series(psi, name='PSI')], axis=1)
            _['TIME'].fillna(time, inplace=True)

            result = pd.concat([result, _], axis=0)

        # Sort the DataFrame by time
        result.sort_values('TIME', ascending=True, inplace=True)

        return result