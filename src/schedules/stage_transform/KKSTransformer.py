import re
from typing import List
# import ValueTransformer# import ValueTransformer

# Трансформер для преобразования значений колонки KKS


class KKSTransformer:
    def __init__(self) -> None:
        self.pattern_0 = r'^[a-zA-Z0-9]+$'
        self.pattern_1 = r'\d+\(\d+(?:\s*,\s*\d+)*\)[a-zA-Z0-9]*'
        self.pattern_2 = r'\d+[a-zA-Z0-9]*\(\d+(?:\s*,\s*\d+)*\)'
        self.pattern_3 = r'\d+\(\d+(?:\s*,\s*\d+)*\)[a-zA-Z0-9]*\(\d+(?:\s*,\s*\d+)*\)'

    def __call__(self, column_as_list: List[str]) -> List[str]:
        transformed_list = []
        for value in column_as_list:
            if type(value) is str:
                raw_kks_list = re.split(r'[\n\s]+', value.strip())

                kks = []
                for raw_kks in raw_kks_list:
                    kks.extend(self.process_one_kks(raw_kks))
                transformed_list.append("\n".join(kks))
            else:
                transformed_list.append(None) # NULL in postgres
        
        return transformed_list
    
    def process_one_kks(self, kks: str) -> List[str]:
        # 10(20)KBF71AA201(100,200,300)
        if re.match(self.pattern_3, kks) is not None:
            base, first_list, body, tail, second_list = self.parse_pattern_3(kks)
            return self.create_kks(base, first_list, body, tail, second_list)
        # 10(20)KBF71AA201
        elif re.match(self.pattern_1, kks) is not None:
            base, first_list, body, tail, second_list = self.parse_pattern_1(kks)
            return self.create_kks(base, first_list, body, tail, second_list)
        # 10KBF71AA201(100,200,300)
        elif re.match(self.pattern_2, kks) is not None:
            base, first_list, body, tail, second_list = self.parse_pattern_2(kks)
            return self.create_kks(base, first_list, body, tail, second_list)
        # 10KBF71AA201 
        elif re.match(self.pattern_0, kks) is not None:
            return [kks]
        
        # Other variants
        if kks in ["prototype", "(prototype)", "NA", "-"]:
            return [kks]
        assert 1==0, f"DON'T match patterns {kks}" 
    
    def parse_pattern_1(self, kks):
        r = re.search(r'\(\d+(?:\s*,\s*\d+)*\)', kks)
        s, e = r.start(), r.end()
        kks = kks[:e+10] # 10(20)PCB03AA501KA02 изюавляемся от концовки -> 10(20)PCB03AA501
        base = kks[:s]
        first_list = kks[s+1:e-1].split(',')
        body = kks[e:-3]
        tail = kks[-3:]
        second_list = []
        return base, first_list, body, tail, second_list

    def parse_pattern_2(self, kks):
        r = re.search(r'\(\d+(?:\s*,\s*\d+)*\)', kks)
        s, e = r.start(), r.end()
        base = kks[:2]
        first_list = []
        body = kks[2:s-3]
        tail = kks[s-3:s]
        second_list = kks[s+1:e-1].split(',')
        return base, first_list, body, tail, second_list

    def parse_pattern_3(self, kks):
        s1, s2, e1, e2 = None, None, None, None
        for i, r in enumerate(re.finditer(r'\(\d+(?:\s*,\s*\d+)*\)', kks)):
            if i == 0: s1, e1 = (r.start(), r.end()) 
            if i == 1: s2, e2 = (r.start(), r.end())
        
        base = kks[:s1]
        first_list = kks[s1+1:e1-1].split(',')
        body = kks[e1:s2-3]
        tail = kks[s2-3:s2]
        second_list = kks[s2+1:e2-1].split(',')
        return base, first_list, body, tail, second_list
        
    def create_kks(
        self,
        head: str = "10",
        first_list: list[str] = ["20", "30"],
        body: str = "KBF71AA",
        tail: str = "201",
        second_list: list[str] = ["100", "200", "300"]):
    
        head_list = [head]
        head_list.extend(first_list)
        tail_list = [tail]
        tail_list.extend(second_list)
    
        kks = []
        for h in head_list:
            for t in tail_list:
                kks.append(str(h) + body + str(t))
        return kks

if __name__ == "__main__":
    print("Example pattern_0")
    print(KKSTransformer().process_one_kks("10KBF71AA201"))
    print()
    print("Example pattern_1")
    print(KKSTransformer().parse_pattern_1("10(20,30)KBF71AA201"))
    print(KKSTransformer().process_one_kks("10(20,30)KBF71AA201"))
    print()
    print("Example pattern_2")
    print(KKSTransformer().parse_pattern_2("10KBF71AA201(100,101,102)"))
    print(KKSTransformer().process_one_kks("10KBF71AA201(100,101,102)"))
    print()
    print("Example pattern_3")
    print(KKSTransformer().parse_pattern_3("10(20,30)KBF71AA201(100,101)"))
    print(KKSTransformer().process_one_kks("10(20,30)KBF71AA201(100,101)"))
