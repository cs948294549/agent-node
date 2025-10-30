import json

with open("../function_snmp/devs.json", "r") as f:
    devs = json.load(f)

dev_dict = {}
for dev in devs:
    dev_dict[dev['ip']] = dev


oid_map_get = {
    "1.3.6.1.2.1.1.5.0": "sysname",
    "1.3.6.1.2.1.1.1.0": "sysdesc",
    "1.3.6.1.2.1.1.6.0": "syscontact",
    "1.3.6.1.2.1.1.4.0": "syscontact",
    "1.3.6.1.2.1.1.3.0": "uptime",
    "1.3.6.1.2.1.1.2.0": "syscontact",
}

oid_map_walk = {
    "1.3.6.1.2.1.2.2.1.2": "portname",
}


def snmpget(ip, community, oid, coding="utf-8"):
    try:
        if ip in dev_dict.keys():
            if oid in oid_map_get.keys():
                return dev_dict[ip][oid_map_get[oid]]
            else:
                print("OID {} not found".format(oid))
                return None
        else:
            print("ip {} not found".format(ip))
    except Exception as e:
        print("snmpget exception=", ip, oid, e)
        return None

def snmpwalk(ip, community, oids, bulk_size=10, coding="utf-8"):
    try:
        if oids in oid_map_walk.keys():
            res = {}
            for i in range(48):
                oid = oids + "." + str(i)
                res[oid] = i
            return res
        else:
            print("OID {} not found".format(oids))
            return None
    except Exception as e:
        print("snmpwalk exception=", ip, oids, e)
        return None