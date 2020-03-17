#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def fetch_records_from_psql(**connection_parameters):
	user = connection_parameters.get("user")
	password = connection_parameters.get("password")
	host = connection_parameters.get("host")
	database = connection_parameters.get("database")
	port = connection_parameters.get("port")

	try:
		connection = psycopg2.connect(user=user, password=password, host=host, database=database, port=port)
		print("Fetching data with Psycopg2")
		cursor = connection.cursor()
		postgreSQL_select_Query = "select age,avg_level_active,avg_level_all from final_aggregate"

		cursor.execute(postgreSQL_select_Query)
		records = cursor.fetchall()

	except (Exception, psycopg2.Error) as error:
		print("Error while fetching data from PostgreSQL", error)

	finally:
		# closing database connection.
		if (connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")
	return records


def main(plot_name="player_progress.png"):
	connection_parameters = {"user": "alari-ThinkPad-P50s",
							 "password": "postgres",
							 "host": "localhost",
							 "port": "5432",
							 "database": "my_db"}
	#get_ipython().run_line_magic('matplotlib', '')
	records = fetch_records_from_psql(**connection_parameters)
	data_to_plot = pd.DataFrame(records, columns=["age", "avg_level_active", "avg_level_all"])
	print(f"The amount of NULL values in the data is {data_to_plot.isnull().sum().sum()}")
	ax = plt.axes()
	sns_plot = sns.lineplot(x='age', y='value', hue='variable',
							data=pd.melt(data_to_plot, ['age']),ax=ax)
	ax.set_title("Player Average Progress as a Function of Player Age")
	figure = sns_plot.get_figure()
	figure.savefig(plot_name)


if __name__ == "__main__":
	main()
