from hsyclass import *

# Download a database containing all data and save to file
# datadump = HSYdatabase(fname='datadump', downlaod=True)

# Downlaod data for single street and save to file
mannerheimintie = HSYdatabase(fname='mansku', download=True, street='Mannerheimintie')

# Restore data from file to new object
new_mansku = HSYdatabase(fname='mansku', download=False)

# Show all addresses on street
print(new_mansku.address['Mannerheimintie'].keys())

# Show all properties on a street number
print(new_mansku.address['Mannerheimintie'][81])

# Show all data on record for property on Mannerheimintie 81
new_mansku.get_propertyobj('103285872A').print_properties()
