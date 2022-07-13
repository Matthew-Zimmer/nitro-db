import json
import sys
from typing import Any, List, Tuple

def readColumnFile(table: str, column: str) -> List[Any]:
    with open(f'{table}/{column}', 'rb') as file:
        data = file.read()
    return [int.from_bytes([data[x + i] for x in range(8)], 'little') for i in range(0, len(data), 8)]

def statColumnFile(table: str, column: str):
    with open(f'{table}/{column}', 'rb') as file:
        data = file.read()
    return {
        'size': len(data)
    }

startPayload = 0
startTable = 2
startDataAttribute = 4
startReferenceAttribute = 6

endPayload = 1
endTable = 3
endDataAttribute = 5
endReferenceAttribute = 7

i8_t = 0
i16_t = 1
i32_t = 2
i64_t = 3
u8_t = 4
u16_t = 5
u32_t = 6
u64_t = 7
string_t = 8
boolean_t = 9
float_t = 10
double_t = 11
reference_t = 12

def typeLength(type):
    if type == i8_t: return 1
    elif type == i16_t: return 2
    elif type == i32_t: return 4
    elif type == i64_t: return 8
    elif type == u8_t: return 1
    elif type == u16_t: return 2
    elif type == u32_t: return 4
    elif type == u64_t: return 8
    elif type == string_t: raise NotImplementedError()
    elif type == boolean_t: return 1
    elif type == float_t: return 4
    elif type == double_t: return 8
    elif type == reference_t: raise RuntimeError('reference type has no length')
    else: raise RuntimeError(f'Unknown type: {type}')

def typeName(type):
    if type == i8_t: return 'i8'
    elif type == i16_t: return 'i16'
    elif type == i32_t: return 'i32'
    elif type == i64_t: return 'i64'
    elif type == u8_t: return 'u8'
    elif type == u16_t: return 'u16'
    elif type == u32_t: return 'u32'
    elif type == u64_t: return 'u64'
    elif type == string_t: return 'string'
    elif type == boolean_t: return 'bool'
    elif type == float_t: return 'float'
    elif type == double_t: return 'double'
    elif type == reference_t: raise RuntimeError('reference type has no name')
    else: raise RuntimeError(f'Unknown type: {type}')
    
def parseU64(data: bytes, i) -> Tuple[int, int]:
    e = i + 8
    if e >= len(data):
        raise RuntimeError(f'payload error expecting at least 8 bytes but only {len(data) - i} left')
    x = int.from_bytes(data[i:e], 'little')
    return e, x

def parseU8(data: bytes, i) -> Tuple[int, int]:
    e = i + 1
    if e >= len(data):
        raise RuntimeError(f'payload error expecting at least 1 byte but only {len(data) - i} left')
    x = int.from_bytes(data[i:e], 'little')
    return e, x

def parseType(data: bytes, i, type, size) -> Tuple[int, List[Any]]:
    l = typeLength(type)
    e = i + l * size
    if e >= len(data):
        raise RuntimeError(f'payload error expecting at least {l*size} byte but only {len(data) - i} left')
    
    if type in { i8_t, i16_t, i32_t, i64_t, u8_t, u16_t, u32_t, u64_t }: 
        return e, [int.from_bytes([data[i + j * l + x] for x in range(l)], 'little') for j in range(0, size)]
    elif type == string_t: 
        raise NotImplementedError()
    elif type == boolean_t: 
        raise NotImplementedError()
    elif type == float_t: 
        raise NotImplementedError()
    elif type == double_t: 
        raise NotImplementedError()
    elif type == reference_t: 
        raise RuntimeError('reference type not basicaly parsable')
    else: raise RuntimeError(f'Unknown type: {type}')

def parseString(data: bytes, i) -> Tuple[int, str]:
    if i >= len(data):
        raise RuntimeError('payload error')
    j, n = parseU64(data, i)
    e = n + j
    s = data[j:e].decode('utf-8')
    return e, s

def parseResult(data, i = 0) -> Tuple[int, Any]:
    if i >= len(data):
        raise RuntimeError('payload error')
    
    if data[i] == startPayload:
        tables = []
        j, o = parseResult(data, i + 1)
        tables.append(o)
        while data[j] != endPayload:
            j, o = parseResult(data, j)
            tables.append(o)
        return j + 1, { 'tables': tables }
    elif data[i] == startTable:
        attrs = []
        j, name = parseString(data, i + 1)
        j, o = parseResult(data, j)
        attrs.append(o)
        while data[j] != endTable:
            j, o = parseResult(data, j)
            attrs.append(o)
        return j + 1, { 'name': name, 'attributes': attrs }
    elif data[i] == startDataAttribute:
        j, name = parseString(data, i + 1)
        j, type = parseU8(data, j)
        j, size = parseU64(data, j)
        j, elements = parseType(data, j, type, size)
        return j + 1, { 'name': name, 'type': typeName(type), 'size': size, 'elements': elements }
    elif data[i] == startReferenceAttribute:
        raise NotImplementedError()
    else:
        raise RuntimeError(f'Unexpected byte: {data[i]}')

def readResult(filename: str):
    with open(filename, 'rb') as file:
        data = file.read()

    n = len(data)

    parsedResults = []
    j, parsedResult = parseResult(data)
    parsedResults.append(parsedResult)
    while j < n:
        j, parsedResult = parseResult(data, j)
        parsedResults.append(parsedResult)

    return {
        'hex': data.hex(),
        'data': parsedResults
    }

def main():
    match sys.argv[1:]:
        case [table, column, 'read']:
            print(json.dumps(readColumnFile(table, column)))
        case [table, column, 'stat']:
            print(json.dumps(statColumnFile(table, column)))
        case [file, 'payload']: 
            print(json.dumps(readResult(file)))
        case _:
            json.dump(sys.argv, sys.stdout)

if __name__ == '__main__':
    main()