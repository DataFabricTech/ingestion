from metadata_collector import metadata_collector_execute
import sys

if __name__ == "__main__":
    pass

    metadata_collector_execute(system_id=sys.argv[1],
    # metadata_collector_execute(system_id='9f0b3f11-7706-4b07-a319-ac7dd49b2071',
                    sink='metadata-rest',
                    sink_host='<INTERNAL_HOST>')

    # metadata_collector_execute(system_id='9f0b3f11-7706-4b07-a319-ac7dd49b2071',
    #                 sink='metadata-rest',
    #                 sink_host='localhost',
    #                 sink_port=8585)

    # metadata_collector_execute(system_name=sys.argv[1], sink="file", sink_host="localhost")
    # metadata_collector_execute(system_id="109ae637-9e13-44f9-9686-a79cf1e12499", sink="file", sink_host="localhost")
    # metadata_collector_execute(system_id="phy-test", sink="file", sink_host="localhost")

    # oracle 910eb81e-7dcd-40c7-8a72-c6ecb134b605
    # postgred 109ae637-9e13-44f9-9686-a79cf1e12499