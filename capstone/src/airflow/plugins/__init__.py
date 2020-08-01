from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class MovieAnalyticsPlugin(AirflowPlugin):
    name = "movie_analytics_plugin"
    operators = [
        operators.StageBigqueryOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]