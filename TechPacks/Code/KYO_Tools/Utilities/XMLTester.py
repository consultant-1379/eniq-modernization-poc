
import KYO_Tools.Utilities as Utils
import os

class XMLTester(object):

    def run(self):
        samplefiles = os.listdir('C:/Users/ebrifol/Dev/NextGen/Data')

        self.listofMOIDs = Utils.odict()
        self.listofCounters = []

        for samplefile in samplefiles:
            XML = Utils.fileToXMLObject('C:/Users/ebrifol/Dev/NextGen/Data/'+samplefile)
            self.parseElements(XML)

        for moid, counters in sorted(self.listofMOIDs.iteritems()):
            print(moid + ' : ' + str(len(counters)))

    def parseElements(self, xmlElement):
        golower = True
        if xmlElement.tag == 'moid':
            moid = xmlElement.text.split(',')[-1].split('=')[0]
            if moid in self.listofMOIDs.keys():
                in_first = set(self.listofCounters)
                in_second = set(self.listofMOIDs[moid])

                in_second_but_not_in_first = in_second - in_first
                self.listofCounters = self.listofCounters + list(in_second_but_not_in_first)

            self.listofMOIDs[moid] = self.listofCounters
            self.listofCounters=[]
            golower = False

        elif xmlElement.tag == 'mt':
            if xmlElement.text not in self.listofCounters:
                self.listofCounters.append(xmlElement.text)
            golower = True

        if golower:
            for child in xmlElement:
                self.parseElements(child)


if __name__ == '__main__':
    test = XMLTester()
    test.run()