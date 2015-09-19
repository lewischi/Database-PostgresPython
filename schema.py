# Jason Pang, Lewis Chi
# 997658594, 996905905
# ECS 165A Homework #4
# Christopher Nitta
# 12/9/2014

import csv
import logging

from connutility import ConnUtility
from dao import Household, Person, Trip, Vehicle, Emission


class Schema:
    def __init__(self, conn, argparser):
        argparser.parse_args(namespace=self)
        self.conn = conn
        self.persons = {}
        self.households = {}
        self.trips = {}
        self.vehicles = {}
        self.emissions = {}


    def setup(self):
        self.setup_sql_creation()
        self.read_csv_data()
        self.perform_sql_insertion()
        pass

    def setup_sql_creation(self):
        # Create the Person table
        logging.info("Creating the Person table...")
        ConnUtility.execute_non_query(self.conn, """
            DROP TABLE IF EXISTS "hw4"."person";
            CREATE TABLE "hw4"."person" (
            "id" int4 NOT NULL
            );
            ALTER TABLE "hw4"."person" ADD PRIMARY KEY ("id");
            """)

        # Create the house table
        logging.info("Creating the Household table...")
        ConnUtility.execute_non_query(self.conn, """
            DROP TABLE IF EXISTS "hw4"."household";
            CREATE TABLE "hw4"."household" (
            "id" int4 NOT NULL
            );
            ALTER TABLE "hw4"."household" ADD PRIMARY KEY ("id");
            """)

        # Create the vehicle table
        logging.info("Creating the Vehicle table...")
        ConnUtility.execute_non_query(self.conn, """
            DROP TABLE IF EXISTS "hw4"."vehicle";
            CREATE TABLE "hw4"."vehicle" (
            "id" int4 NOT NULL,
            "mpg" float8 NOT NULL
            );
            ALTER TABLE "hw4"."vehicle" ADD PRIMARY KEY ("id");
            """)

        # Create the trip table
        logging.info("Creating the Trip table...")
        ConnUtility.execute_non_query(self.conn, """
            DROP TABLE IF EXISTS "hw4"."trip";
            CREATE TABLE "hw4"."trip" (
            "id" int4 NOT NULL,
            "person_id" int4 NOT NULL,
            "household_id" int4 NOT NULL,
            "vehicle_id" int4 NOT NULL,
            "miles" float8,
            "yearmonth" char(6) NOT NULL
            );
            ALTER TABLE "hw4"."trip" ADD PRIMARY KEY ("id");
            ALTER TABLE "hw4"."trip" ADD FOREIGN KEY ("person_id") REFERENCES "hw4"."person" ("id") ON DELETE CASCADE ON UPDATE CASCADE;
            """)

        # Create the emission table
        logging.info("Creating the Emission table...")
        ConnUtility.execute_non_query(self.conn, """
            DROP TABLE IF EXISTS "hw4"."emission";
           CREATE TABLE "hw4"."emission" (
            "yearmonth" varchar(6) NOT NULL,
            "value" float8 NOT NULL,
            "columnorder" int4 NOT NULL,
            PRIMARY KEY ("yearmonth", "columnorder")
            );
            """)

        pass

    def read_csv_data(self):

        # Reading DAYV2PUB.CSV
        csv_path = self.csv_dir + 'DAYV2PUB.CSV'

        logging.info("Reading CSV file @ %s..." % csv_path)
        with open(csv_path) as csv_file_dayv2pub:
            reader_dayv2pub = csv.reader(csv_file_dayv2pub)
            next(reader_dayv2pub)  # Ignore first row

            for idx, row in enumerate(reader_dayv2pub):
                household_id = int(row[0])  # HOUSEID
                person_id = int(row[1])  # PERSONID
                trip_miles = float(row[94])  # TRPMILES
                vehicle_id = int(row[83])  # VEHID
                yearmonth = str(row[93])  # TDAYDATE

                self.persons[int(row[1])] = Person(person_id)
                self.trips[idx] = Trip(idx, household_id, person_id, vehicle_id, trip_miles, yearmonth)

        # Reading VEHV2PUB.CSV
        csv_path = self.csv_dir + 'VEHV2PUB.CSV'

        logging.info("Reading CSV file @ %s..." % csv_path)
        with open(csv_path) as csv_file_vehv2pub:
            reader_vehv2pub = csv.reader(csv_file_vehv2pub)
            next(reader_vehv2pub)  # Ignore first row

            for idx, row in enumerate(reader_vehv2pub):
                household_id = int(row[0])  # HHID
                vehicle_id = int(row[2])  # VEHID
                mpg = float(row[55])

                self.households[int(row[0])] = Household(household_id)
                self.vehicles[vehicle_id] = Vehicle(vehicle_id, mpg)

        # Reading EIA_CO2_Transportation_2014.csv
        csv_path = self.csv_dir + 'EIA_CO2_Transportation_2014.csv'

        logging.info("Reading CSV file @ %s..." % csv_path)
        with open(csv_path) as csv_file_eia_co2_transport:
            reader_eia_co2_transport = csv.reader(csv_file_eia_co2_transport)
            next(reader_eia_co2_transport)  # Ignore first row

            for idx, row in enumerate(reader_eia_co2_transport):
                yearmonth = str(row[1])  # YYYYMM
                value = float(row[2])  # Value
                column_order = int(row[3])  # Column_Order

                self.emissions[yearmonth + str(column_order)] = Emission(yearmonth, value, column_order)


    def perform_sql_insertion(self):

        household_insert_str = ''
        person_insert_str = ''
        trip_insert_str = ''
        vehicle_insert_str = ''
        emission_insert_str = ''

        logging.info('Inserting values into Household table...')
        for idx, household in enumerate(self.households.values()):
            household_insert_str += 'INSERT INTO "hw4"."household" VALUES (' + str(household.id) + ');\n'  #
            # HOUSEID

            if idx % self.flush_after == 0 and idx > 0:
                logging.debug('Flushing objects to the database (total flushed so far: %d).' % idx)
                ConnUtility.execute_non_query(self.conn, household_insert_str)
                self.conn.commit()
                household_insert_str = ''

        if household_insert_str:
            ConnUtility.execute_non_query(self.conn, household_insert_str)
            self.conn.commit()
        logging.info('Finished writing all Household table values.')

        logging.info('Inserting values into Person table...')
        for idx, person in enumerate(self.persons.values()):
            person_insert_str += 'INSERT INTO "hw4"."person" VALUES (' + str(person.id) + ');\n'  # PERSONID

            if idx % self.flush_after == 0 and idx > 0:
                logging.debug('Flushing objects to the database (total flushed so far: %d).' % idx)
                ConnUtility.execute_non_query(self.conn, person_insert_str)
                self.conn.commit()
                person_insert_str = ''

        if person_insert_str:
            ConnUtility.execute_non_query(self.conn, person_insert_str)
            self.conn.commit()

        logging.info('Finished writing all Person table values.')

        logging.info('Inserting values into Vehicle table...')
        for idx, vehicle in enumerate(self.vehicles.values()):
            vehicle_insert_str += 'INSERT INTO "hw4"."vehicle" VALUES ('
            vehicle_insert_str += "'" + str(vehicle.id) + "', "
            vehicle_insert_str += "'" + str(vehicle.mpg) + "', "
            vehicle_insert_str = vehicle_insert_str[:-2]  # Remove the last ", "
            vehicle_insert_str += ');' + '\n'

            if idx % self.flush_after == 0 and idx > 0:
                logging.debug('Flushing objects to the database (total flushed so far: %d).' % idx)
                ConnUtility.execute_non_query(self.conn, vehicle_insert_str)
                self.conn.commit()
                vehicle_insert_str = ''

        if vehicle_insert_str:
            ConnUtility.execute_non_query(self.conn, vehicle_insert_str)
            self.conn.commit()

        logging.info('Finished writing all Vehicle table values.')

        logging.info('Inserting values into Trip table...')
        for idx, trip in enumerate(self.trips.values()):
            trip_insert_str += 'INSERT INTO "hw4"."trip" VALUES ('
            trip_insert_str += "'" + str(trip.id) + "', "
            trip_insert_str += "'" + str(trip.person_id) + "', "
            trip_insert_str += "'" + str(trip.household_id) + "', "
            trip_insert_str += "'" + str(trip.vehicle_id) + "', "
            trip_insert_str += "'" + str(trip.miles) + "', "
            trip_insert_str += "'" + str(trip.yearmonth) + "', "
            trip_insert_str = trip_insert_str[:-2]  # Remove the last ", "
            trip_insert_str += ');' + '\n'

            if idx % self.flush_after == 0 and idx > 0:
                logging.debug('Flushing objects to the database (total flushed so far: %d).' % idx)
                ConnUtility.execute_non_query(self.conn, trip_insert_str)
                self.conn.commit()
                trip_insert_str = ''

        if trip_insert_str:
            ConnUtility.execute_non_query(self.conn, trip_insert_str)
            self.conn.commit()

        logging.info('Finished writing all Trip table values.')

        logging.info('Inserting values into Emission table...')
        for idx, emission in enumerate(self.emissions.values()):
            emission_insert_str += 'INSERT INTO "hw4"."emission" VALUES ('
            emission_insert_str += "'" + str(emission.yearmonth) + "', "
            emission_insert_str += "'" + str(emission.value) + "', "
            emission_insert_str += "'" + str(emission.columnorder) + "', "
            emission_insert_str = emission_insert_str[:-2]  # Remove the last ", "
            emission_insert_str += ');' + '\n'

            if idx % self.flush_after == 0 and idx > 0:
                logging.debug('Flushing objects to the database (total flushed so far: %d).' % idx)
                ConnUtility.execute_non_query(self.conn, emission_insert_str)
                self.conn.commit()
                emission_insert_str = ''

        if emission_insert_str:
            ConnUtility.execute_non_query(self.conn, emission_insert_str)
            self.conn.commit()

        logging.info('Finished writing all Emission table values.')

