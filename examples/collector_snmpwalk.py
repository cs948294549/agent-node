from function_snmp.snmp_collector import snmp_get, snmp_walk


if __name__ == '__main__':
    ip = "10.162.0.14"
    community = "public"
    oid = "1.3.6.1.2.1.1.1.0"
    oid1 = "1.3.6.1.2.1.31.1.1.1.15"

    res = snmp_get(ip, community, oid=oid)
    print(res)

    res1 = snmp_walk(ip, community, oid=oid1)
    print(res1)