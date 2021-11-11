import pdb
import os
import argparse

class FlowData:
    def __init__(self, src_comp, dest_comp, src_port, dst_port, start_time):
        self._src_comp = src_comp
        self._dest_comp = dest_comp
        self._duration = 0
        self._time = start_time
        self._src_port = src_port
        self._dst_port = dst_port
        self._byte_count = 0
        self._packet_count = 0
        self._num_connections = 0
        self._identifier = f"{dest_comp}"

    def dump(self):
        print_string = f"[{self._src_comp}:{self._src_port} -> {self._dest_comp}:{self._dst_port} [start_ts : {self._time}, duration: {self._duration}, byte_count: {self._byte_count}, num_connections: {self._num_connections}]\n"
        print(print_string)

    def export(self, flow_id_map, out_file):
        export_string = f"{flow_id_map[self._src_comp]},{self._src_comp},{flow_id_map[self._dest_comp]},{self._dest_comp},{self._src_port},{self._dst_port},{self._time},{self._duration},{self._byte_count},{self._num_connections}\n"
        out_file.write(export_string)

    def add_duration(self, duration):
        self._duration += duration

    def add_throughput(self, packet_count, byte_count):
        self._byte_count += byte_count
        self._packet_count += packet_count
        self._num_connections += 1

    def get_identifier(self):
        return self._identifier

    @staticmethod
    def get_identity(dest_comp, src_port, dst_port):
        return f"{dest_comp}"
        #return f"{dest_comp}_{src_port}_{dst_port}"


class FlowProcessor:
    def __init__(self, flows_file, start_day, end_day, num_bucket_size):
        self._file = flows_file
        self._id_map = {}
        self._flow_map = {}
        self._id_out_file = "flows_id.txt"
        self._flow_links = "flow_links_1.csv"
        self._start_time = start_day * 3600 * 24
        self._end_time = (end_day+1) * 3600 * 24
        self._bucket_size = num_bucket_size * 3600
        self._total_connections = 0
        self._flow_links = f"flow_links_{start_day}_{end_day}_{num_bucket_size}.csv"
        print(f"Generating import file for flows between day {start_day} to day {end_day+1} with hour bucket size {num_bucket_size} exporting data into file {self._flow_links}\n")

    def generate_ids(self):
        if os.path.exists(self._id_out_file):
            print("Id file already exists. Using the file for loading ids\n")
            self.load_ids()
        else:
            comp_id = 0
            num_lines = 0
            with open(self._file) as fp:
                while True:
                    line = fp.readline()
                    if not line:
                        print("empty line")
                        break

                    num_lines = num_lines + 1
                    tokens = line.split(",")
                    src_comp = tokens[2]
                    dst_comp = tokens[4]
                    if src_comp not in self._id_map:
                        comp_id += 1
                        self._id_map[src_comp] = comp_id

                    if dst_comp not in self._id_map:
                        comp_id = comp_id + 1
                        self._id_map[dst_comp] = comp_id

            print(f"Processed {num_lines} and found {comp_id} unique computers\n")
            self.create_computer_id_file()


    def create_computer_id_file(self):
        with open(self._id_out_file, "w") as fp:
            for comp in self._id_map:
                id_line = f"{comp}, {self._id_map[comp]}\n"
                fp.write(id_line)

            fp.close()

    def load_ids(self):
        with open(self._id_out_file, "r") as fp:
            while True:
                line = fp.readline()
                if not line:
                    break

                tokens = line.split(",")
                self._id_map[tokens[0]] = tokens[1].strip()

    def generate_import_file(self):
        write_file = open(self._flow_links, "w")
        header = f"src_comp_id,src_comp,dst_comp_id,dst_comp,src_port,dst_port,start_time,duration,byte_count,num_connections\n"
        write_file.write(header)

        with open(self._file) as fp:
            num_lines = 0
            bucket = 0
            while True:
                line = fp.readline()
                if not line:
                    break

                num_lines += 1
                if num_lines % 100000 == 0:
                    print(f"Processed {num_lines}")

                tokens = line.split(",")
                start_time = int(tokens[0])
                if start_time < self._start_time:
                    continue
                if start_time > self._end_time:
                    print(f"Generated {self._total_connections}")
                    break

                if start_time/self._bucket_size > bucket:
                    bucket += 1
                    self._total_connections += self.dump_connections(export=True, outfile=write_file)

                self.create_connection(tokens)


    def create_connection(self, tokens):
        # tokens
        # 0: timestamp
        # 1: duration
        # 2: source
        # 3: source_port
        # 4: destination
        # 5: dest_port
        # 6: protocol
        # 7: packet_count
        # 8: byte_count
        src_comp = tokens[2]
        dest_comp = tokens[4]
        src_port = tokens[3]
        dest_port = tokens[5]
        start_time = int(tokens[0])

        if src_comp not in self._flow_map:
            self._flow_map[src_comp] = {}

        flow_id = FlowData.get_identity(dest_comp, src_port, dest_port)
        if flow_id not in self._flow_map[src_comp]:
            self._flow_map[src_comp][flow_id] = FlowData(src_comp, dest_comp, src_port, dest_port, start_time)

        flow_data = self._flow_map[src_comp][flow_id]
        flow_data.add_duration(int(tokens[1]))
        flow_data.add_throughput(int(tokens[7]), int(tokens[8]))

    def dump_connections(self, export=False, outfile=None):
        print(f"Dumping flow map size: {len(self._flow_map)}")
        total = 0
        for src_comp in self._flow_map:
            for flow_id in self._flow_map[src_comp]:
                total += 1
                if not export:
                    self._flow_map[src_comp][flow_id].dump()
                else:
                    self._flow_map[src_comp][flow_id].export(self._id_map, outfile)

        self._flow_map.clear()
        return total

def main():
    print("Starting to process")
    parser = argparse.ArgumentParser()
    parser.add_argument("start_day", help="starting day", default=0, type=int)
    parser.add_argument("end_day", help="end day", default=100, type=int)
    parser.add_argument("num_hours_bucket", help="Number of hours per bucket", default=1, type=int)
    args = parser.parse_args()
    flow_processor = FlowProcessor("flows.txt", args.start_day, args.end_day, args.num_hours_bucket)
    flow_processor.generate_ids()
    flow_processor.generate_import_file()

if __name__ == "__main__":
    main()
