

class HSYdatabase():
    def __init__(self, street=None, fname=None, download=False,
        url='https://kartta.hsy.fi/geoserver/wfs', 
        version='1.1.0', 
        featuretype='asuminen_ja_maankaytto:pks_rakennukset_paivittyva'):
        # Fname - file to reqad/write from/to
        # Download - 
        # read
        
        self.properties = {}
        self.address = {}
        self.duplicates = []
        self.url = url
        self.version = version
        self.featuretype = featuretype
        # Do not write to file, stream
        if fname is not None: 
            if download is True:
                if street is None:
                    self.write_HSYdump(fname)
                else:
                    self.write_HSYstreet(fname, street)
            else:
                self.create_fromfile(fname)
        else:
            self.create_fromstream(street)
        self.create_addressdict()
        return

    def create_addressdict(self):
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
        from xml.etree import cElementTree as ElementTree
        self.xmltodict(ElementTree.parse(fname).getroot())

    def create_fromstream(self, street=None):
        from xml.etree import cElementTree as ElementTree
        self.xmltodict(ElementTree.fromstring(self.stream_HSYdata(street)))

    def write_HSYdump(self, fname, read=True):
        self.write_HSYdata(fname)
        if read is True:
            self.xmltodict(ElementTree.parse(fname).getroot())

    def write_HSYstreet(self, fname, street, read=True):
        self.write_HSYdata(fname, street)
        if read is True:
            self.xmltodict(ElementTree.parse(fname).getroot())

    def stream_HSYdump(self):
        return self.stream_HSYdata()

    def stream_HSYstreet(self, street):
        return self.stream_HSYdata(street)

    def stream_HSYdata(self, street=None):
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
        data = self.stream_HSYdata(fname, street)
        # Write data into gml file 
        out = open(fname, 'wb') 
        out.write(data) 
        out.close()

    def xmltodict(self, etree):
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
        return self.properties[identifier].address()


class Property():
    def __init__(self, propertydict):
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
        for key, value in self.__dict__.items():
            print('{}:'.format(key).ljust(15), value)

def read_xmlfile(fname):
    from xml.etree import cElementTree as ElementTree
    return ELementTree.parse(fname).getroot()
 
def read_hsy(street, fname='hsy/{}.gml'): 
    from owslib.fes import PropertyIsLike 
    from owslib.etree import etree 
    from owslib.wfs import WebFeatureService
    wfs11 = WebFeatureService(url='https://kartta.hsy.fi/geoserver/wfs', version='1.1.0') 
 
    make_filter = PropertyIsLike(propertyname='katu', literal=street) 
    filterxml = etree.tostring(make_filter.toXML()).decode("utf-8") 
    response = wfs11.getfeature(typename='asuminen_ja_maankaytto:pks_rakennukset_paivittyva', filter=filterxml) 
    #write data into gml file 
    out = open(fname.format(street), 'wb') 
    out.write(response.read()) 
    out.close() 
 
def dump_hsy(fname='hsy/dump.gml'): 
    from owslib.fes import PropertyIsLike 
    from owslib.etree import etree 
    from owslib.wfs import WebFeatureService
    wfs11 = WebFeatureService(url='https://kartta.hsy.fi/geoserver/wfs', version='1.1.0') 
    response = wfs11.getfeature(typename='asuminen_ja_maankaytto:pks_rakennukset_paivittyva')     
    #write data into gml file 
    out = open(fname, 'wb') 
    out.write(response.read()) 
    out.close()


def xmltodict(etree, properties = {}):
    from copy import deepcopy
    duplicates = []
    for child in etree:
        if len(child[0]) != 0:
            _, duplicates = xmltodict(child, properties)
        else:
            propdata= {}
            for item in child:
                propdata[item.tag.split('}')[-1]] = item.text
            number = int(float(propdata['osno1']))
            try:
                properties[propdata['katu']]
            except:
                if 'katu' in propdata.keys():
                    properties[propdata['katu']] = {}
                else:
                    if 'N/A' not in properties.keys():
                        properties['N/A'] = {}
                    properties['N/A'][int(propdata['raktun'])] = propdata
                    continue 
            try:
                properties[propdata['katu']][number]
            except:
                properties[propdata['katu']][number] = {}
            raktun = propdata['vtj_prt']
            try:
                properties[propdata['katu']][number][raktun]
                duplicates.append([propdata['katu'], number, raktun])
            except:
                properties[propdata['katu']][number][raktun] = propdata
    return properties, duplicates

    


