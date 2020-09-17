import sqlite3

CHARACTER_SUBCLASSES = ['necromancer', 'cleric', 'fighter', 'mage', 'thief']

database = None

# How many total Characters are there?
def query_1():
  """
  Determines the number of total Characters.
  """

  query_character_count = """
  SELECT 
    COUNT(*)
  FROM charactercreator_character
  """

  return database.execute(query_character_count).fetchone()[0]


# How many of each specific subclass?
def query_2():
  """
  Determines the number of Characters for each character subclass.
  """

  def query_subclass(subclass):
    """
    Determines the number of Characters for the specified subclass.
    """
    query = query_character_subclass_count.format(subclass)
    return database.execute(query).fetchone()[0]

  query_character_subclass_count = """
  SELECT 
    COUNT(*)
  FROM charactercreator_{0}
  """

  return [(sc, query_subclass(sc)) for sc in CHARACTER_SUBCLASSES]
    

# How many total Items?
def query_3():
  """
  Determines the total number of Items.
  """

  query_item_count = """
  SELECT 
    COUNT(*) 
  FROM armory_item
  """

  return database.execute(query_item_count).fetchone()[0]


# How many of the Items are weapons? How many are not?
def query_4():
  """
  Determines the number of Items that are Weapons
  and the number of Items that are not Weapons.
  """

  query_weapon_count = """
  SELECT 
    COUNT(*) 
  FROM armory_weapon
  """

  query_non_weapon_count = """
  SELECT 
    COUNT(*) 
  FROM armory_item 
  LEFT JOIN armory_weapon 
  ON armory_item.item_id = armory_weapon.item_ptr_id
  WHERE armory_weapon.item_ptr_id IS NULL
  """

  return [database.execute(query).fetchone()[0] 
    for query in (query_weapon_count, query_non_weapon_count)]


# How many Items does each character have? (Return first 20 rows)
def query_5():
  """
  Determines the number of Items each Character has for
  the first 20 rows.
  """

  query_item_count_by_character = """
  SELECT
    characters.character_id,
    COUNT(item_id)
  FROM charactercreator_character as characters
  LEFT JOIN charactercreator_character_inventory as inventory
  ON characters.character_id = inventory.character_id
  GROUP BY characters.character_id
  LIMIT 20
  """

  return list(database.execute(query_item_count_by_character))


# How many Weapons does each character have? (Return first 20 rows)
def query_6():
  """
  Displays the number of Weapons that each Character
  has for the first 20 rows.
  """

  query_weapon_count_by_character = """
  SELECT
    characters.character_id,
    COUNT(armory.item_ptr_id)
  FROM charactercreator_character as characters
  LEFT JOIN charactercreator_character_inventory as inventory 
  ON characters.character_id = inventory.character_id
  LEFT JOIN armory_weapon as armory
  ON inventory.item_id = armory.item_ptr_id
  GROUP BY characters.character_id
  LIMIT 20
  """

  return list(database.execute(query_weapon_count_by_character))


# On average, how many Items does each Character have?
def query_7():
  """
  Determines the average number of Items each Character has.
  """

  query_avg_items = """
  SELECT
    AVG(item_counts)
  FROM (
    SELECT
      COUNT(item_id) as item_counts
    FROM charactercreator_character_inventory
    GROUP BY character_id)
  """

  return database.execute(query_avg_items).fetchone()[0]


# On average, how many Weapons does each character have?
def query_8():
  """
  Determines the average number of weapons each Character has.
  """

  query_avg_weapons = """
  SELECT
    AVG(weapon_counts)
  FROM (
    SELECT
      COUNT(armory.item_ptr_id) as weapon_counts
    FROM charactercreator_character as characters
    LEFT JOIN charactercreator_character_inventory as inventory 
    ON characters.character_id = inventory.character_id
    LEFT JOIN armory_weapon as armory
    ON inventory.item_id = armory.item_ptr_id
    GROUP BY characters.character_id)
  """

  return database.execute(query_avg_weapons).fetchone()[0]


def display_answer(title, ans, display_func=None):
  """
  Displays the title and answer for a question.
  """
  print(title.title())
  print('-' * len(title))

  if display_func is None:
    print(ans)
  else:
     display_func(ans)

  print()


def display_subclasses(subclasses):
  for subclass, count in subclasses:
    print('{0:13} {1}'.format(f'{subclass.capitalize()}s:', count))


def display_item_counts(item_counts):
  print('Weapons:', item_counts[0])
  print('Non-Weapons:', item_counts[1])

def display_counts(counts):
  for char_id, count in counts:
    print('character_id {0:2d}:{1:2d}'.format(char_id, count))


def display_avg(avg):
  print('{0:.3f}'.format(avg))


QUERY_TITLES = ['number of characters',
                'number of characters by subclass',
                'number of total items',
                'number of weapons',
                'number of items for first 20 characters',
                'number of weapons for first 20 characters',
                'average number of items',
                'average number of weapons']


DISPLAY_FUNCS = {
  2: display_subclasses,
  4: display_item_counts, 
  5: display_counts,
  6: display_counts,
  7: display_avg,
  8: display_avg
}


if __name__ == '__main__':
  database = sqlite3.connect("rpg_db.sqlite3")
  for i, title in enumerate(QUERY_TITLES, 1):
    display_answer(title, eval(f'query_{i}()'), DISPLAY_FUNCS.get(i, None))
