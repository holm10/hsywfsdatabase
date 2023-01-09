

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
    write_HSYdump(self, fname, read=True)
        Dumps data from wfs server and creates properties database
    write_HSYstreet(self, fname, street, read=True)
        Downloads street from wfs server and creates properties database
    stream_HSYdump(self)
        Downloads data from wfs to memory, creates properties database
    stream_HSYstreet(self, street)
        Downloads street from wfs to memory, creates properties database
    stream_HSYdata(self, street=None)
        Returns string with street data, dump if street is None
    write_HSYdata(self, fname, street=None)
        Writes street to fname, dump if street is None
    xmltodict(self, etree)
        Recursively parses XML ElementTree to properties dictionary 
    get_propertyaddress(self, identifier)
        Returns address of property with identifier in database
    '''
    def __init__(self, street=None, fname=None, download=False,
        url='https://kartta.hsy.fi/geoserver/wfs', 
        version='1.1.0', 
        featuretype='asuminen_ja_maankaytto:pks_rakennukset_paivittyva'):

        self.properties = {}
        self.address = {}
        self.duplicates = []
        self.url = url
        self.version = version
        self.featuretype = featuretype
        # Open file for read/write
        if fname is not None: 
            # Download data to file and read it
            if download is True:
                if street is None:
                    self.write_HSYdump(fname)
                else:
                    self.write_HSYstreet(fname, street)
            # Do not download, read only
            else:
                self.create_fromfile(fname)
        # Do not write to file, stream
        else:
            self.create_fromstream(street)
        # Create address lookup dictionary
        self.create_addressdict()
        return

    def create_addressdict(self):
        ''' Creates address dictionary for lookup purposes '''
        for key, element in self.properties.items():
            street, number = element.address()
            try:
                self.address[street]
            except:
                self.address[street] = {}
            try:
                self.address[street][number]
            except:
                self.address[street][number] = []
            self.address[street][number].append(key) 

    def create_fromfile(self, fname):
        ''' Reads property data from file and creates property database '''
        from xml.etree import cElementTree as ElementTree
        self.xmltodict(ElementTree.parse(fname).getroot())

    def create_fromstream(self, street=None):
        ''' Creates properties database by downloading from wfs server '''
        from xml.etree import cElementTree as ElementTree
        self.xmltodict(ElementTree.fromstring(self.stream_HSYdata(street)))

    def write_HSYdump(self, fname, read=True):
        ''' Dumps data from wfs server and creates properties database '''
        self.write_HSYdata(fname)
        if read is True:
            self.xmltodict(ElementTree.parse(fname).getroot())

    def write_HSYstreet(self, fname, street, read=True):
        ''' Downloads street from wfs server and creates properties database '''
        self.write_HSYdata(fname, street)
        if read is True:
            self.xmltodict(ElementTree.parse(fname).getroot())

    def stream_HSYdump(self):
        ''' Downloads data from wfs server to memory, creates properties db '''
        return self.stream_HSYdata()

    def stream_HSYstreet(self, street):
        ''' Downloads street from wfs to memory, creates properties database '''
        return self.stream_HSYdata(street)

    def stream_HSYdata(self, street=None):
        ''' Returns string with street data, dump if street is None '''
        from owslib.fes import PropertyIsLike 
        from owslib.etree import etree 
        from owslib.wfs import WebFeatureService

        wfs11 = WebFeatureService(url=self.url, version=self.version) 
        if street is not None:
            make_filter = PropertyIsLike(propertyname='katu', literal=street) 
            filterxml = etree.tostring(make_filter.toXML()).decode("utf-8") 
        else:
            filterxml = None
        response = wfs11.getfeature(typename=self.featuretype, 
            filter=filterxml)
        return response.read()

    def write_HSYdata(self, fname, street=None):
        ''' Writes street to fname, dump if street is None '''
        data = self.stream_HSYdata(fname, street)
        # Write data into gml file 
        out = open(fname, 'wb') 
        out.write(data) 
        out.close()

    def xmltodict(self, etree):
        ''' Recursively parses XML ElementTree to properties dictionary '''
        from copy import deepcopy
        for child in etree:
            if len(child[0]) != 0:
                self.xmltodict(child)
            else:
                propdata= {}
                for item in child:
                    propdata[item.tag.split('}')[-1]] = item.text
                try:
                    self.properties[propdata['vtj_prt']]
                    self.duplicates.append(Property(propdata))
                except:
                    self.properties[propdata['vtj_prt']] = Property(propdata)
        return

    def get_propertyaddress(self, identifier):
        ''' Returns address of property with identifier in database '''
        return self.properties[identifier].address()


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
        for key, value in propertydict.items():
            try:
                value = float(value)
                if mod(value, 1) < 1e-6:
                    setattr(self, key, int(value))
                else:
                    setattr(self, key, value)
            except:
                setattr(self, key, value)
            # Overwrite with string in case of identifier
            if ('tun' in key) or (key in ['vtj_prt', 'posno']):
                setattr(self, key, value)
    
    def address(self):
        ''' Returns the street name and number '''
        try:
            return self.katu, self.osno1
        except:
            if hasattr(self, 'katu'):
                return self.katu, None
            elif hasattr(self, 'osno1'):
                return None, self.osno1
            else:
                return None, None

    def print_properties(self):
        ''' Prints all properties of property '''
        for key, value in self.__dict__.items():
            print('{}:'.format(key).ljust(15), value)

