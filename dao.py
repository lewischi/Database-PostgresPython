# Jason Pang, Lewis Chi
# 997658594, 996905905
# ECS 165A Homework #4
# Christopher Nitta
# 12/9/2014


class Person:
    def __init__(self, id):
        self.id = id


class Household:
    def __init__(self, id):
        self.id = id


class Trip:
    def __init__(self, id, household_id, person_id, vehicle_id, miles, yearmonth):
        self.id = id
        self.household_id = household_id
        self.person_id = person_id
        self.vehicle_id = vehicle_id
        self.miles = miles
        self.yearmonth = yearmonth

class Vehicle:
    def __init__(self, id, mpg):
        self.id = id
        self.mpg = mpg

class Emission:
    def __init__(self, yearmonth, value, columnorder):
        self.yearmonth = yearmonth
        self.value = value
        self.columnorder = columnorder