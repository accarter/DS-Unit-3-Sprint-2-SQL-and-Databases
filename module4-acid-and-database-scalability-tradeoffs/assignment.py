from elephant_pipeline import ElephantPipeline
import re

pipeline = ElephantPipeline()

survived_query = """
SELECT
    SUM(survived)
FROM
    titanic
"""

died_query = """
SELECT
    COUNT(*)
FROM
    titanic
WHERE
    survived=0
"""

survived_by_class_query = """
SELECT
    pclass,
    SUM(survived)
FROM
    titanic
GROUP BY
    pclass
"""

died_by_class_query = """
SELECT
    pclass,
    COUNT(*)
FROM
    titanic
WHERE
    survived=0
GROUP BY
    pclass
"""

avg_survivor_age_query = """
SELECT
    AVG(age)
FROM
    titanic
WHERE
    survived=1
"""

avg_non_survivor_age_query = """
SELECT
    AVG(age)
FROM
    titanic
WHERE
    survived=0
"""

avg_age_by_class = """
SELECT
    pclass,
    AVG(age)
FROM
    titanic
GROUP BY
    pclass
"""

avg_fare_by_class = """
SELECT
    pclass,
    AVG(fare)
FROM
    titanic
GROUP BY
    pclass
"""

avg_fare_by_survival = """
SELECT
    survived,
    AVG(fare)
FROM
    titanic
GROUP BY
    survived
"""

avg_rel_by_status = """
SELECT
    {0},
    AVG({1})
FROM
    titanic
GROUP BY
    {0}
"""

duplicate_names_query = """
SELECT
    name,
    COUNT(*)
FROM
    titanic
GROUP BY
    name
HAVING
    COUNT(*) > 1
"""

num_couples_query = """
SELECT (
    name
) FROM
    titanic
WHERE
    Parents_Children_Aboard > 0
"""

def request_count(query):
    return pipeline.make_query(query)[0][0]

num_survived = request_count(survived_query)
print("Survivors:", num_survived)

num_died = request_count(died_query)
print("Fatalities:", num_died)

print(survived_by_class_query)
print("Survivors by class:", pipeline.make_query(survived_by_class_query))
print("Fatalities by class:", pipeline.make_query(died_by_class_query))
print("Average survivor age:", pipeline.make_query(avg_survivor_age_query))
print("Average non-survivor age:", pipeline.make_query(avg_non_survivor_age_query))
print("Average age by class:", pipeline.make_query(avg_age_by_class))
print("Average fare by class:", pipeline.make_query(avg_fare_by_class))
print("Average fare by survival:", pipeline.make_query(avg_fare_by_survival))

relationships = ['Siblings_Spouses_Aboard', 'Parents_Children_Aboard']
statuses = ['survived', 'pclass']

for rel in relationships:
    for status in statuses:
        print(f"Average {rel} by {status}:",
            pipeline.make_query(avg_rel_by_status.format(status, rel)))

print("Duplicate names:", pipeline.make_query(duplicate_names_query))

names = [name[0] for name in pipeline.make_query(num_couples_query)]
honorifics = set(name.split()[0] for name in names)

women = [name for name in names if name.split()[0] in ('Mrs.', 'Miss.')]
men = [name for name in names if name.split()[0] in ('Rev.', 'Capt.', 'Mr.', 'Master.')]

spouse_name_pat = r"(Mrs.|Miss.|Rev.|Capt.|Mr.|Master.)\s(.*)\((.*)\)(.*)"
male_name_pat = r"(Mrs.|Miss.|Rev.|Capt.|Mr.|Master.)\s(\w*)\s+(\w*)"
prog = re.compile(spouse_name_pat)


married_men = []

for woman in women:
    result = prog.match(woman)
    if result is not None:
        married_men.append(" ".join(
        (result.group(2).strip() + " " + result.group(4).strip()).split(" ")))

num_couples = 0
for man in men:
    man = man[man.index(".") + 1:]
    man = man.strip(" ")
    if man in married_men:
        num_couples += 1

print(num_couples)
