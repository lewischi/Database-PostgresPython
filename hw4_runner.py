# Jason Pang, Lewis Chi
# 997658594, 996905905
# ECS 165A Homework #4
# Christopher Nitta
# 12/9/2014


import json
import argparse
import csv
import os
import psycopg2
import logging

from program_base import ProgramBase
from connutility import ConnUtility
from schema import Schema


class Hw4Runner(ProgramBase):

    def run(self):
        self.conn = ConnUtility.connect()

        if self.rebuild:
            self.setup_schema()

        self.print_assignment_header()
        self.part_a()
        self.part_b()
        self.part_c()


    def setup_schema(self):
        ConnUtility.wipe_and_rebuild_schema(self.conn)
        schema = Schema(self.conn, self.argparser)
        schema.setup()

    def print_assignment_header(self):
        print 'ECS 165A Homework #4 \n' \
              '-------------------- \n' \
              '\n' \
              'Instructor: Christopher Nitta \n' \
              'Names: Jason Pang, Lewis Chi \n' \
              'IDs: 997658594, 996905905 \n' \
              'Date: December 11, 2014 \n' \
              '\n'

    def part_a(self):
        print "a) Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile " \
              "increment (e.g. 5, 10, 15,  ..., 95, 100)."
        print ''

        for mile_target in range(5, 105, 5):
            query = "SELECT " \
                    "CAST(num_target as decimal)/ CAST(total_count as decimal) * 100 " \
                    "AS solution " \
                    "FROM " \
                    "( " \
                    "		SELECT COUNT(*) as num_target " \
                    "		FROM " \
                    "			( " \
                    "			SELECT SUM(miles) as miles " \
                    "			FROM \"hw4\".\"trip\" " \
                    "			WHERE miles >= 0 " \
                    "			GROUP BY person_id, household_id " \
                    "			) AS q2 " \
                    "		WHERE miles < " + str(mile_target) + " " \
                                                                  ") AS table1, " \
                                                                  "( " \
                                                                  "		SELECT COUNT(*) as total_count " \
                                                                  "		FROM " \
                                                                  "			( " \
                                                                  "			SELECT SUM(miles) as miles " \
                                                                  "			FROM \"hw4\".\"trip\" " \
                                                                  "			GROUP BY person_id, household_id " \
                                                                  "			) AS q2 " \
                                                                  ") AS table2"
            query_return = ConnUtility.execute_query_scalar(self.conn, query)
            print "% individuals traveling < " + str(mile_target) + " miles/day: " + str(query_return[0])

    def part_b(self):
        print "b) Calculate the average fuel economy of all miles traveled for trips less than specific distances " \
              "from previous problem. Only consider trips that utilize a household vehicle (VEHID is 1 or larger), " \
              "use the EPA combined fuel economy (EPATMPG) for the particular vehicle"
        print ''

        for mile_target in range(5, 105, 5):
            query = "" \
                    "SELECT Cast(total_miles AS DECIMAL) / Cast(total_gallons AS DECIMAL) AS " \
                    "       solution " \
                    "FROM   (SELECT SUM(miles) AS total_miles " \
                    "        FROM " \
                    "							 ( " \
                    "                SELECT *, (miles / mpg) AS miles_over_mpg " \
                    "                FROM " \
                    "											( " \
                    "                        SELECT miles, mpg " \
                    "											  FROM \"hw4\".\"trip\" " \
                    "												INNER JOIN \"hw4\".\"vehicle\" on \"hw4\".\"trip\".\"vehicle_id\" = \"hw4\".\"vehicle\".\"id\" " \
                    "												WHERE vehicle_id >= 1 " \
                    "												AND miles >= 0 " \
                    "												AND miles < " + str(mile_target) + " " \
                    "											) AS q1 " \
                    "							 ) AS q2) AS table1, " \
                    "       (SELECT SUM(miles_over_mpg) AS total_gallons " \
                    "        FROM " \
                    "							 ( " \
                    "                SELECT *, (miles / mpg) AS miles_over_mpg " \
                    "                FROM " \
                    "											( " \
                    "                        SELECT miles, mpg " \
                    "											  FROM \"hw4\".\"trip\" " \
                    "												INNER JOIN \"hw4\".\"vehicle\" on \"hw4\".\"trip\".\"vehicle_id\" = \"hw4\".\"vehicle\".\"id\" " \
                    "												WHERE vehicle_id >= 1 " \
                    "												AND miles >= 0 " \
                    "												AND miles < " + str(mile_target) + " " \
                    "											) AS q1 " \
                    "							 ) AS q2 ) AS table2"
            query_return = ConnUtility.execute_query_scalar(self.conn, query)
            print "Average fuel economy for trips < " + str(mile_target) + " miles/day: " + str(query_return[0])

    def part_c(self):
        print 'c) Calculate the percent of transportation CO2 emissions that should be attributed to household ' \
              'vehicles for each month fo the survey (3/2008 - 04/2009).'
        print ''

        months_to_use = ['200803', '200804', '200805', '200806', '200807', '200808', '200809', '200810', '200811',
                         '200812', '200901', '200902', '200903', '200904']

        for month in months_to_use:
            query = (""
                     "            SELECT Cast(total_co2 AS DECIMAL) / Cast(total_value AS DECIMAL) * 100 AS "
                     "       solution "
                     "FROM   (SELECT SUM(miles_over_mpg) * 0.008887 * 117538000 * 31 / (SELECT COUNT(DISTINCT "
                     "household_id) FROM \"hw4\".\"trip\" WHERE YEARMONTH='" + month + "') AS total_co2"
                     "        FROM   "
                     "                             ("
                     "                SELECT *, (miles / mpg) AS miles_over_mpg"
                     "                FROM   "
                     "                                            ("
                     "                        SELECT miles, mpg"
                     "                                              FROM \"hw4\".\"trip\" "
                     "                                                INNER JOIN \"hw4\".\"vehicle\" on \"hw4\".\"trip\".\"vehicle_id\" = \"hw4\".\"vehicle\".\"id\""
                     "                                                WHERE vehicle_id >= 1"
                     "                                                AND miles >= 0"
                     "                                                AND yearmonth = '" + month + "'"
                     "                                            ) AS q1"
                     "                             ) AS q2) AS table1, "
                     "       (SELECT value * 1000000 AS total_value FROM \"hw4\".\"emission\" WHERE yearmonth='" +
                     month + "'AND "
                     "columnorder=12) AS q3")

            query_return = ConnUtility.execute_query_scalar(self.conn, query)
            print "% of transportation CO2 emissions for month " + month + ": " + str(query_return[0])
