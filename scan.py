#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bluepy.btle import Scanner, DefaultDelegate


# In[2]:


CAR_TYPES = {
    8: 'Ground Shock',
    9: 'Skull',
    10: 'Thermo',
    11: 'Nuke',
    12: 'Guardian',
    15: 'Free Wheel',
    16: 'X52',
    17: 'X52 Ice'
}


# In[3]:


from struct import unpack

class ScanDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)

    def handleDiscovery(self, dev, isNewDev, isNewData):
        if isNewDev:
            print("Discovered device", dev.addr)
        elif isNewData:
            print("Received new data from", dev.addr)

scanner = Scanner().withDelegate(ScanDelegate())
devices = scanner.scan(2.0)

for dev in devices:
    data = {adtype: (desc, value) for adtype, desc, value in dev.getScanData()}
    try:
        if data[7][1] != 'be15beef-6186-407e-8381-0bd89c4d8df4':
            print('Skipping %r' % data[7][1])
            continue
    except KeyError:
        continue

    carId, = unpack('xxxBxxxx', bytearray.fromhex(data[255][1]))
    carType = CAR_TYPES[carId]
  
    print('Device %s %s' % (dev.addr, carType))
#     print("Device %s (%s), RSSI=%d dB" % (dev.addr, dev.addrType, dev.rssi))
#     for (adtype, desc, value) in dev.getScanData():
#         print("  (%s) %s = %s" % (adtype, desc, value))

