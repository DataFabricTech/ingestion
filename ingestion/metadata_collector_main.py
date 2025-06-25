import sys
from metadata_collector import metadata_collector_execute

if __name__ == "__main__":
    pass

    # metadata_collector_execute(system_id=sys.argv[1],
    #                 sink='metadata-rest')

    metadata_collector_execute(system_id='38fe4f2a-4030-4770-8dc5-e2fe21009789',
                    sink='metadata-rest')

    # metadata_collector_execute(system_name=sys.argv[1], sink="file")
    # metadata_collector_execute(system_id="51de6bd5-085b-4aad-bac2-d35adb44c18b", sink="file")
    # metadata_collector_execute(system_id="tibero_phy", sink="file")
    # metadata_collector_execute(system_id="altibase_phy", sink="file")

