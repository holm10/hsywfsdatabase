

class HSYdatabase():
    ''' Class for creating and interacting with HSY databases 

    Attributes
    ----------
    url - str
        URL to wfs database
    version - str
        wfs version requested
    featuretype - str
        feature to be accessed from wfs database
    properties - dict
        properties database, sorted by vtj_prt key
    address - dict
        registry of properties by address, osno1. Memebers are lists of
        vtj_prt registered at each address
    duplicates - list
        list of objects with duplicate vtj_prt values. First instance is
        stored in properties, second instance is stored to duplicates

    Methods
    -------
    create_addressdict(self)
        Creates address dictionary from properties for lookup purposes
    create_fromfile(self, fname)
        Reads property data from file and creates property database
    create_fromstring(self, street)
        Creates properties database by downloading from wfs server
    write_HSY(self, fname, street=None, read=True)
        Dumps data from wfs server and creates properties database
    stream_HSY(self, street=None)
        Returns string with street data, dump if street is None
    write__to_file(self, fname, street=None)
        Writes street to fname, dump if street is None
    xmltodict(self, etree)
        Recursively parses XML ElementTree to properties dictionary 
    get_propertyaddress(self, identifier)
        Returns address of property with identifier in database
    '''
    def __init__(self, street=None, fname=None, download=False,
        url='https://kartta.hsy.fi/geoserver/wfs', version='1.1.0', 
        featuretype='asuminen_ja_maankaytto:pks_rakennukset_paivittyva'):
        ''' Initializes HSY database object 

        Optional arguments
        ------------------
        street - str [None]
            Street to retrieve from database. If None, restores/
            retrieves all data available in database
        fname - str [None]
            Path to local HSY database to restore. If none, opens API 
            connection and reads data into memory
        download - bool [False]
            Switch to specify whether to download data from API server
            or look for stored XML data file under name fname. If set 
            to True, any retrieved data will be saved to XML file at 
            fname 
'''

        # Set upp dictionaries to help accessing data by different keys
        self.properties = {}
        self.address = {}
        # Initialize duplicates list
        self.duplicates = []
        # Store API url to object
        self.url = url
        # Store API version to object
        self.version = version
        # Store requested featuretype to object
        self.featuretype = featuretype
        # Check whether a file read/write is requested
        if fname is not None: 
            # Download data to file and read it
            if download is True:
                self.write_HSY(fname, street)
            # Do not download, read data from XML file
            else:
                self.create_fromfile(fname)
        # If not, stream data directly to memory without
        # reading/writing from/to file
        else:
            self.create_fromstream(street)
        # Create address lookup dictionary
        self.create_addressdict()
        return

    def create_addressdict(self):
        ''' Creates address dictionary for lookup purposes '''
        
        # Go through each property in the database
        for key, element in self.properties.items():
            # Store the street name and number of the Property
            # object to temporary variables
            street, number = element.address()
            # Check that address dictionary entry for the Property
            # street exists: if it does not, create it as a dictionary
            try:
                self.address[street]
            except:
                self.address[street] = {}
            # Now, check whether the street already has any properies
            # listed at the address. If not, create and empty list for
            # storing all properties at this address
            try:
                self.address[street][number]
            except:
                self.address[street][number] = []
            # Store the 'vtj_prt' identifiers for the address
            # to the address entry
            self.address[street][number].append(key) 

    def create_fromfile(self, fname):
        ''' Reads property data from file and creates property database
    
        Arguments
        ---------
        fname : str
            String with path to xml datafile to be read into properties 
            dictionary
        '''
        from xml.etree import cElementTree as ElementTree
        # In a oneliner: pass an ElementTree's root, created from file
        # fname, to the xmltodict function. xmltodict reads the data in the 
        # file, and creates properties based on the data. The data is stored
        # in properties, using their 'vtj_prt' values as identifier
        self.xmltodict(ElementTree.parse(fname).getroot())

    def create_fromstream(self, street=None):
        ''' Creates properties database by downloading from wfs server
    
        Optional arguments
        ------------------
        street : str [None]
            String containing street name to be fetched from API and 
            stored to the properties dictionary
        '''
        from xml.etree import cElementTree as ElementTree
        # In a oneliner: Pass ElementTree's root, created from HSY API data
        # stream, to the xmltodict function. xmltodict reads the data in the 
        # file, and creates properties based on the data. The data is stored
        # in properties, using their 'vtj_prt' values as identifier
        self.xmltodict(ElementTree.fromstring(self.stream_HSY(street)))


    def write_HSY(self, fname, street=None, read=True):
        ''' Downloads data from wfs server and creates properties database 

        Arguments
        ---------
        fname : str
            String/path to file where to write the downloaded HSY data

        Optional arguments
        ------------------
        street : str [None]
            Define street to write/read. If none, writes/reads all 
            available data
        read : bool [True]
            Swith defining whether to read the data just saved into the
            properties dictionary.


        '''
        # Download the HSY data from the API server, and save it to fname
        self.write_to_file(fname, street)
        # Check if written data should be saved to object
        if read is True:
            # If yes, read data into properties dictionary
            self.create_fromfile(fname)

    def stream_HSY(self, street=None):
        ''' Returns string with street data, dump if street is None 

        Optional arguments
        ------------------
        street : str [None]
            Define street to write/read. If none, streams all available 
            data
        '''
        from owslib.fes import PropertyIsLike 
        from owslib.etree import etree 
        from owslib.wfs import WebFeatureService

        # Open gateway to the HSY API
        wfs11 = WebFeatureService(url=self.url, version=self.version) 
        # Check whether to filter for street name
        if street is not None:
            # Create a PropertyIsLike object from to the propoertyname 'katu'
            make_filter = PropertyIsLike(propertyname='katu', literal=street) 
            # Create an XML filter from PropertyIsLike object
            filterxml = etree.tostring(make_filter.toXML()).decode("utf-8") 
        else:
            # If no street requested, create None XML filter
            filterxml = None
        # Get the response object from the API portal, with any applied filter
        response = wfs11.getfeature(typename=self.featuretype, 
            filter=filterxml)
        # TODO: add checks to ensure response is good, or throw error otherwise
        # Return all data from the API portal in a single string
        return response.read()

    def write_to_file(self, fname, street=None):
        ''' Writes street to fname, dump if street is None 

        Arguments
        ---------
        fname : str
            String/path to file where to write the downloaded HSY data

        Optional arguments
        ------------------
        street : str [None]
            Define street to write/read. If none, writes/reads all 
            available data
        '''
        # Store data from API portal to data
        data = self.stream_HSY(fname, street)
        # Open file with path/name fname for writing
        out = open(fname, 'wb') 
        # Write XML data into binary file
        out.write(data) 
        # Close the files
        out.close()

    def xmltodict(self, etree):
        ''' Recursively parses XML ElementTree to properties dictionary '''
        from copy import deepcopy
    
        # Iterate over every entry in etree
        for child in etree:
            # The object has more levels, read recursively
            if len(child[0]) != 0:
                self.xmltodict(child)
            # This is the data-storage level of the file: store data
            else:
                # Create a temporary dictionary for storing propery data
                propdata= {}
                # Go through all data stored for this property
                for item in child:
                    # The item tag ends in the parameter name: split the
                    # tag on curly bracket and use end as key in dictionary.
                    # The property text as string is the data entry for key
                    propdata[item.tag.split('}')[-1]] = item.text
                # All data has been read: create property object using data
                # just read ands stored to propdata
                try:
                    # Check if the identifier already exists: if it does not
                    # an error is produced and the exception is executed. 
                    # If no error is produced, evaluate the remaining lines 
                    # and skip the exception
                    self.properties[propdata['vtj_prt']]
                    # If the entry exists, leave the original entry and append
                    # the new entry to the duplicates list
                    self.duplicates.append(Property(propdata))
                except:
                    # If the object does not already exist, create it and 
                    # store it in the properties dictionary with 'vrj_prt'
                    # as key to access it.
                    self.properties[propdata['vtj_prt']] = Property(propdata)
        return

    def get_propertyaddress(self, identifier):
        ''' Returns address of property with identifier in database '''
        return self.properties[identifier].address()

    def get_propertyobj(self, identifier):
        ''' Returns Property object with identifier in database '''
        return self.properties[identifier]


class Property():
    ''' Class containing property information

    Attributes
    ----------
    Dynamically attributed from database

    Methods
    -------
    address(self)
        Returns the street name and number
    print_properties(self)
        Prints all properties of property
    '''
    def __init__(self, propertydict):
        ''' Initializes the property data from dict 
        
        Dictionary keys are stored as attributes with entries as data.
        Arguments are converted to int or float wherever possible. Only
        the *tun* identifiers and vtj_prt and posno* attributes are
        retained as str.

        Arguments
        ---------
        propertydict - dict
            Dictionary contaning all data pertaiing to property
        '''
        from numpy import mod

        # For every entry in the property data dictionary, i.e. for all data 
        # supplied from teh API, save the data to the Property object
        for key, value in propertydict.items():
            # Try to cast the data to int or float
            try:
                # See if the data string contains numerical data> if not,
                # go to exception and store string. If data can be cast as
                # float, proceed with the operations
                value = float(value)
                # Check whether the data is within a millionth of an integer:
                # necessary as all ints are stored as floats in the database,
                # and some get numerical noise.
                if abs(mod(value, 1)) < 1e-6:
                    # Value is assumed to be an integer: set the parameter
                    # name, as given by the API, to int of value
                    setattr(self, key, int(value))
                else:
                    # Value is not integer, but float: set the parameter
                    # name, as given by the API, to a float
                    setattr(self, key, value)
            except:
                # Data is not numerical: set the parameter name, as given by 
                # the API, to be the original data string
                setattr(self, key, value)
            # Overwrite with string in case of identifier: these are also
            # numerical, but may start with zeros: casting them as floats
            # removes leading zeros, resulting in potentially non-unique
            # identifiers, as well as issues when searching the database
            if ('tun' in key) or (key in ['vtj_prt', 'posno']):
                # Any key contaning 'tun', or matching 'vtj_prt' or 'posno'
                # are assumed to be idetifiers
                setattr(self, key, value)
    
    def address(self):
        ''' Returns the street name and number '''
        # Try to return data directly, if it was present in the database
        try:
            return self.katu, self.osno1
        # All data requested was not available, return the data available
        except:
            # Check if katu exists
            if hasattr(self, 'katu'):
                # If it does, return street and return None for street number
                return self.katu, None
            # Otherwise, see if the Property has a street number but no name
            elif hasattr(self, 'osno1'):
                # If that is the case, return the number and none for street
                return None, self.osno1
            # Finally, it might be neither exist: return None for both
            else:
                return None, None

    def print_properties(self):
        ''' Prints all properties of property '''
        # Loop throough all data entires for Property
        for key, value in self.__dict__.items():
            # Print the propery names and values
            print('{}:'.format(key).ljust(15), value)

