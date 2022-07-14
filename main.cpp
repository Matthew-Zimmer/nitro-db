#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include <exception>
#include <filesystem>
#include <string.h>

using byte = std::uint8_t;
using bytes = std::vector<byte>;

bool verbose = false;

enum class AttributeKind : byte {
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    string,
    boolean,
    float_,
    double_,
    reference,
};

std::uint8_t attributeSize(AttributeKind k) {
    switch (k)
    {
    case AttributeKind::i8:
        return 1;
    case AttributeKind::i16:
        return 2;
    case AttributeKind::i32:
        return 4;
    case AttributeKind::i64:
        return 8;
    case AttributeKind::u8:
        return 1;
    case AttributeKind::u16:
        return 2;
    case AttributeKind::u32:
        return 4;
    case AttributeKind::u64:
        return 8;
    case AttributeKind::string:
        throw "todo";
    case AttributeKind::boolean:
        return 1;
    case AttributeKind::float_:
        return 4;
    case AttributeKind::double_:
        return 8;
    case AttributeKind::reference:
        return 4;
    }
    throw std::runtime_error("unhandled attr kind");
}

struct Attribute {
    AttributeKind kind;
    union Attribute_ {
        std::int8_t i8;
        std::int16_t i16;
        std::int32_t i32;
        std::int64_t i64;
        std::uint8_t u8;
        std::uint16_t u16;
        std::uint32_t u32;
        std::uint64_t u64;
        std::string string;
        bool boolean;
        float float_;
        double double_;
        std::uint64_t reference;

        Attribute_() {}
        ~Attribute_() {}
    } data;

    Attribute() = default;

    Attribute(Attribute const& attr) : kind(attr.kind) {
        switch (kind)
        {
        case AttributeKind::i8:
            data.i8 = attr.data.i8;
            break;
        case AttributeKind::i16:
            data.i16 = attr.data.i16;
            break;
        case AttributeKind::i32:
            data.i32 = attr.data.i32;
            break;
        case AttributeKind::i64:
            data.i64 = attr.data.i64;
            break;
        case AttributeKind::u8:
            data.u8 = attr.data.u8;
            break;
        case AttributeKind::u16:
            data.u16 = attr.data.u16;
            break;
        case AttributeKind::u32:
            data.u32 = attr.data.u32;
            break;
        case AttributeKind::u64:
            data.u64 = attr.data.u64;
            break;
        case AttributeKind::string:
            data.string = attr.data.string;
            break;
        case AttributeKind::boolean:
            data.boolean = attr.data.boolean;
            break;
        case AttributeKind::float_:
            data.float_ = attr.data.float_;
            break;
        case AttributeKind::double_:
            data.double_ = attr.data.double_;
            break;
        case AttributeKind::reference:
            data.reference = attr.data.reference;
            break;
        }
    }

    Attribute& operator=(Attribute const& attr) {
        kind = attr.kind;
        switch (kind)
        {
        case AttributeKind::i8:
            data.i8 = attr.data.i8;
            break;
        case AttributeKind::i16:
            data.i16 = attr.data.i16;
            break;
        case AttributeKind::i32:
            data.i32 = attr.data.i32;
            break;
        case AttributeKind::i64:
            data.i64 = attr.data.i64;
            break;
        case AttributeKind::u8:
            data.u8 = attr.data.u8;
            break;
        case AttributeKind::u16:
            data.u16 = attr.data.u16;
            break;
        case AttributeKind::u32:
            data.u32 = attr.data.u32;
            break;
        case AttributeKind::u64:
            data.u64 = attr.data.u64;
            break;
        case AttributeKind::string:
            data.string = attr.data.string;
            break;
        case AttributeKind::boolean:
            data.boolean = attr.data.boolean;
            break;
        case AttributeKind::float_:
            data.float_ = attr.data.float_;
            break;
        case AttributeKind::double_:
            data.double_ = attr.data.double_;
            break;
        case AttributeKind::reference:
            data.reference = attr.data.reference;
            break;
        }
        return *this;
    }
};


enum ControlMessage : byte {
    startPayload,
    endPayload,
    startTable,
    endTable,
    startDataAttribute,
    endDataAttribute,
    startReferenceAttribute,
    endReferenceAttribute,
};

template <typename T>
void serialize(T const& i, bytes& v) {
    std::copy(reinterpret_cast<byte const*>(&i), reinterpret_cast<byte const*>(&i) + sizeof(T), std::back_inserter(v));
}

void serialize(std::string const& s, bytes& v) {
    serialize(s.size(), v);
    std::copy(s.begin(), s.end(), std::back_inserter(v));
}

void serialize(AttributeKind const& k, bytes& v) {
    serialize(static_cast<std::uint8_t>(k), v);
}

enum class InstructionKind {
    selectTable,
    createTable,
    createColumn,
    selectColumn,
    readColumn,
    appendColumn,
    end,
    send,
    open,
    close,
    sort,
    free,
};

enum class PayloadKind : byte {
    payload,
    table,
    data,
    ref,
};

struct Instruction {
    InstructionKind kind;
    union Instruction_ {
        struct { std::string name; } createTable;
        struct { std::string name; } selectTable;
        struct { std::string name; AttributeKind type; } createColumn;
        struct { std::string name; } selectColumn;
        struct {} readColumn;
        struct { Attribute attr;  } appendColumn;
        struct {} end;
        struct {} send;
        struct { PayloadKind kind; } open;
        struct { PayloadKind kind; } close;
        struct {  } sort;
        struct {  } free;

        Instruction_() {}
        ~Instruction_() {}
    } data;

    Instruction() = default;

    Instruction(Instruction const& i) : kind(i.kind) {
        switch (kind)
        {
        case InstructionKind::selectTable:
            data.selectTable = i.data.selectTable;
            break;
        case InstructionKind::createTable:
            data.createTable = i.data.createTable;
            break;
        case InstructionKind::createColumn:
            data.createColumn = i.data.createColumn;
            break;
        case InstructionKind::selectColumn:
            data.selectColumn = i.data.selectColumn;
            break;
        case InstructionKind::readColumn:
            data.readColumn = i.data.readColumn;
            break;
        case InstructionKind::appendColumn:
            data.appendColumn = i.data.appendColumn;
            break;
        case InstructionKind::end:
            data.end = i.data.end;
            break;
        case InstructionKind::send:
            data.send = i.data.send;
            break;
        case InstructionKind::open:
            data.open = i.data.open;
            break;
        case InstructionKind::close:
            data.close = i.data.close;
            break;
        case InstructionKind::sort:
            data.sort = i.data.sort;
            break;
        case InstructionKind::free:
            data.free = i.data.free;
            break;
        }
    }
};

#define abortIfFails(x) if (auto e = x; !e.empty()) return e

std::string str(AttributeKind const& k) {
    switch (k) {
    case AttributeKind::i8:
        return "i8";
    case AttributeKind::i16:
        return "i16";
    case AttributeKind::i32:
        return "i32";
    case AttributeKind::i64:
        return "i64";
    case AttributeKind::u8:
        return "u8";
    case AttributeKind::u16:
        return "u16";
    case AttributeKind::u32:
        return "u32";
    case AttributeKind::u64:
        return "u64";
    case AttributeKind::string:
        return "string";
    case AttributeKind::boolean:
        return "boolean";
    case AttributeKind::float_:
        return "float";
    case AttributeKind::double_:
        return "double";
    case AttributeKind::reference:
        return "ref";
    }
    throw std::system_error();
}

std::string str(Attribute const& attr) {
    switch (attr.kind) {
    case AttributeKind::i8:
        return std::to_string(attr.data.i8) + ": i8";
    case AttributeKind::i16:
        return std::to_string(attr.data.i16) + ": i16";
    case AttributeKind::i32:
        return std::to_string(attr.data.i32) + ": i32";
    case AttributeKind::i64:
        return std::to_string(attr.data.i64) + ": i64";
    case AttributeKind::u8:
        return std::to_string(attr.data.u8) + ": u8";
    case AttributeKind::u16:
        return std::to_string(attr.data.u16) + ": u16";
    case AttributeKind::u32:
        return std::to_string(attr.data.u32) + ": u32";
    case AttributeKind::u64:
        return std::to_string(attr.data.u64) + ": u64";
    case AttributeKind::string:
        return attr.data.string + ": string";
    case AttributeKind::boolean:
        return std::string(attr.data.boolean ? "true" : "false")  + ": bool";
    case AttributeKind::float_:
        return std::to_string(attr.data.float_) + ": float";
    case AttributeKind::double_:
        return std::to_string(attr.data.double_) + ": double";
    case AttributeKind::reference:
        return "ref";
    }
    throw std::system_error();
}

std::string str(PayloadKind p) {
    switch (p) {
    case PayloadKind::payload: return "payload";
    case PayloadKind::table: return "table";
    case PayloadKind::data: return "data";
    case PayloadKind::ref: return "ref";
    }
    throw std::system_error();
}

void print(Instruction const& i) {
    switch (i.kind)
    {
    case InstructionKind::selectTable:
        return static_cast<void>(std::cout << "select table " << i.data.selectTable.name << std::endl);
    case InstructionKind::createTable:
        return static_cast<void>(std::cout << "create table " << i.data.createTable.name << std::endl);
    case InstructionKind::createColumn:
        return static_cast<void>(std::cout << "create column " << i.data.createColumn.name << ": " << str(i.data.createColumn.type) << std::endl);
    case InstructionKind::selectColumn:
        return static_cast<void>(std::cout << "select column " << i.data.selectColumn.name << std::endl);
    case InstructionKind::readColumn:
        return static_cast<void>(std::cout << "read" << std::endl);
    case InstructionKind::appendColumn:
        return static_cast<void>(std::cout << "append " << str(i.data.appendColumn.attr)  << std::endl);
    case InstructionKind::end:
        return static_cast<void>(std::cout << "end" << std::endl);
    case InstructionKind::send:
        return static_cast<void>(std::cout << "send" << std::endl);
    case InstructionKind::open:
        return static_cast<void>(std::cout << "open " << str(i.data.open.kind) << std::endl);
    case InstructionKind::close:
        return static_cast<void>(std::cout << "close " << str(i.data.open.kind) << std::endl);
    case InstructionKind::sort:
        return static_cast<void>(std::cout << "sort" << std::endl);
    case InstructionKind::free:
        return static_cast<void>(std::cout << "free" << std::endl);
    }
}

struct ColumnInfo {
    AttributeKind type;
    std::uint64_t count;

    ColumnInfo() = default;
    ColumnInfo(AttributeKind k, std::uint64_t c) : type(k), count(c) {}
};

struct TableInfo {
    std::unordered_map<std::string, ColumnInfo> columns;
};

void loadAttrDataFromBytes(Attribute& d, char* b) {
    switch (d.kind)
    {
    case AttributeKind::i8:
        return static_cast<void>(std::copy(b, b + 1, reinterpret_cast<char*>(&d.data.i8)));
    case AttributeKind::i16:
        return static_cast<void>(std::copy(b, b + 2, reinterpret_cast<char*>(&d.data.i16)));
    case AttributeKind::i32:
        return static_cast<void>(std::copy(b, b + 4, reinterpret_cast<char*>(&d.data.i32)));
    case AttributeKind::i64:
        return static_cast<void>(std::copy(b, b + 8, reinterpret_cast<char*>(&d.data.i64)));
    case AttributeKind::u8:
        return static_cast<void>(std::copy(b, b + 1, reinterpret_cast<char*>(&d.data.u8)));
    case AttributeKind::u16:
        return static_cast<void>(std::copy(b, b + 2, reinterpret_cast<char*>(&d.data.u16)));
    case AttributeKind::u32:
        return static_cast<void>(std::copy(b, b + 4, reinterpret_cast<char*>(&d.data.u32)));
    case AttributeKind::u64:
        return static_cast<void>(std::copy(b, b + 8, reinterpret_cast<char*>(&d.data.u64)));
    case AttributeKind::string:
        throw "todo";
    case AttributeKind::boolean:
        return static_cast<void>(std::copy(b, b + 1, reinterpret_cast<char*>(&d.data.boolean)));
    case AttributeKind::float_:
        return static_cast<void>(std::copy(b, b + 4, reinterpret_cast<char*>(&d.data.float_)));
    case AttributeKind::double_:
        return static_cast<void>(std::copy(b, b + 8, reinterpret_cast<char*>(&d.data.double_)));
    case AttributeKind::reference:
        throw "todo";
    }
}

struct DataBase {
private:
    // vm registers
    std::string table;
    std::string column;
    std::vector<std::uint64_t> ordering;
    std::vector<Attribute> data;
    bytes payload;

    // vm state
    std::unordered_map<std::string, TableInfo> tables;
    std::string dumpFile;

    std::string columnFileName(std::string const& table, std::string const& column) {
        return table + "/" + column;
    }
   
    void createTableFile(std::string const& table) {
        std::filesystem::create_directory(table);
    }

    void createColumnFile(std::string const& table, std::string const& column) {
        std::ofstream f(columnFileName(table, column));
        f.flush();
    }

    std::fstream openColumnFile(std::string const& table, std::string const& column) {
        return std::fstream(columnFileName(table, column), std::ios::binary);
    }

    std::fstream appendColumnFile(std::string const& table, std::string const& column) {
        return std::fstream(columnFileName(table, column), std::ios::binary | std::ios::app);
    }

    std::uint64_t columnCount(std::string const& table, std::string const& column) {
        return tables[table].columns[column].count;
    }

    std::uint64_t addColumnCount(std::string const& table, std::string const& column, std::uint64_t amount) {
        return tables[table].columns[column].count += amount;
    }

    AttributeKind columnType(std::string const& table, std::string const& column) {
        return tables[table].columns[column].type;
    }

    void readAttributes(FILE* fd, std::vector<Attribute>& d, std::uint64_t count, AttributeKind type) {
        auto n = d.size();
        d.resize(n + count);
        auto s = attributeSize(type);
        auto ss = s * count;
        char* b = new char[ss];
        fread(b, 1, ss, fd);

        std::uint64_t j = 0;
        for (auto i = n; i < n + count; i++, j += s) {
            d[i].kind = type;
            loadAttrDataFromBytes(d[i], b + j);
        }

        delete[] b;
    }

public:
    DataBase(std::string const& dumpFile) : dumpFile(dumpFile) {}

    void clearState() {
        table = "";
        column = "";
        ordering.clear();
        data.clear();
        payload.clear();
        data.shrink_to_fit();
        payload.shrink_to_fit();
    }

    std::string createTable(std::string const& name) {
        if (tables.contains(name))
            return "Table: " + name + " already exists";

        tables[name] = TableInfo();
        createTableFile(name);

        return "";
    }

    std::string createColumn(std::string const& name, AttributeKind const& type) {
        if (tables[table].columns.contains(name))
            return "Column: " + name + " already exists on table" + table;

        tables[table].columns[name] = ColumnInfo(type, 0);

        createColumnFile(table, name);
        
        return "";
    }

    std::string selectTable(std::string const& name) {
        if (!tables.contains(name))
            return "Cannot select an non existent table named: " + name;
        
        table = name;

        return "";
    }

    std::string selectColumn(std::string const& name) {
        if (!tables[table].columns.contains(name))
            return "Cannot select an non existent column named: " + name + " on table " + table;
        
        column = name;

        return "";
    }  

    std::string readColumn() {
        auto f = fopen(columnFileName(table, column).c_str(), "rb");
        auto c = columnCount(table, column);
        auto t = columnType(table, column);
        readAttributes(f, data, c, t);

        fclose(f);

        return "";
    }

    std::string appendColumn(Attribute const& attr) {
        auto f = appendColumnFile(table, column);
        auto s = attributeSize(attr.kind);
        switch (attr.kind) {
        case AttributeKind::i8:  f.write(reinterpret_cast<char const*>(&attr.data.i8), s); break;
        case AttributeKind::i16: f.write(reinterpret_cast<char const*>(&attr.data.i16), s); break;
        case AttributeKind::i32: f.write(reinterpret_cast<char const*>(&attr.data.i32), s); break;
        case AttributeKind::i64: f.write(reinterpret_cast<char const*>(&attr.data.i64), s); break;
        case AttributeKind::u8:  f.write(reinterpret_cast<char const*>(&attr.data.u8), s); break;
        case AttributeKind::u16: f.write(reinterpret_cast<char const*>(&attr.data.u16), s); break;
        case AttributeKind::u32: f.write(reinterpret_cast<char const*>(&attr.data.u32), s); break;
        case AttributeKind::u64: f.write(reinterpret_cast<char const*>(&attr.data.u64), s); break;
        case AttributeKind::boolean: f.write(reinterpret_cast<char const*>(&attr.data.boolean), s); break;
        case AttributeKind::float_: f.write(reinterpret_cast<char const*>(&attr.data.float_), s); break;
        case AttributeKind::double_: f.write(reinterpret_cast<char const*>(&attr.data.double_), s); break;
        case AttributeKind::string: return "Todo append string column";
        case AttributeKind::reference: f.write(reinterpret_cast<char const*>(&attr.data.reference), s); break;
        }

        addColumnCount(table, column, 1);

        return "";
    }

    // copies the data from data to payload
    // assumes that there is only one column loaded
    std::string send() {
        auto count = columnCount(table, column);
        auto type = columnType(table, column);
        serialize(column, payload);
        serialize(type, payload);
        serialize(count, payload);
        switch (type) {
        case AttributeKind::i8:        if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.i8, payload); }        else { for (auto&& x : data) serialize(x.data.i8, payload); } break;
        case AttributeKind::i16:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.i16, payload); }       else { for (auto&& x : data) serialize(x.data.i16, payload); } break;
        case AttributeKind::i32:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.i32, payload); }       else { for (auto&& x : data) serialize(x.data.i32, payload); } break;
        case AttributeKind::i64:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.i64, payload); }       else { for (auto&& x : data) serialize(x.data.i64, payload); } break;
        case AttributeKind::u8:        if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.u8, payload); }        else { for (auto&& x : data) serialize(x.data.u8, payload); } break;
        case AttributeKind::u16:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.u16, payload); }       else { for (auto&& x : data) serialize(x.data.u16, payload); } break;
        case AttributeKind::u32:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.u32, payload); }       else { for (auto&& x : data) serialize(x.data.u32, payload); } break;
        case AttributeKind::u64:       if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.u64, payload); }       else { for (auto&& x : data) serialize(x.data.u64, payload); } break;
        case AttributeKind::boolean:   if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.boolean, payload); }   else { for (auto&& x : data) serialize(x.data.boolean, payload); } break;
        case AttributeKind::float_:    if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.float_, payload); }    else { for (auto&& x : data) serialize(x.data.float_, payload); } break;
        case AttributeKind::double_:   if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.double_, payload); }   else { for (auto&& x : data) serialize(x.data.double_, payload); } break;
        case AttributeKind::string:    if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.string, payload); }    else { for (auto&& x : data) serialize(x.data.string, payload); } break;
        case AttributeKind::reference: if (!ordering.empty()) { for (std::uint64_t idx : ordering) serialize(data[idx].data.reference, payload); } else { for (auto&& x : data) serialize(x.data.reference, payload); } break;
        }

        return "";
    }

    std::string open(PayloadKind k) {
        switch (k)
        {
        case PayloadKind::payload: payload.push_back(static_cast<byte>(ControlMessage::startPayload)); break;
        case PayloadKind::table: payload.push_back(static_cast<byte>(ControlMessage::startTable)); serialize(table, payload); break;
        case PayloadKind::data: payload.push_back(static_cast<byte>(ControlMessage::startDataAttribute)); break;
        case PayloadKind::ref: payload.push_back(static_cast<byte>(ControlMessage::startReferenceAttribute)); break;
        }
        return "";
    }

    std::string close(PayloadKind k) {
        switch (k)
        {
        case PayloadKind::payload: payload.push_back(static_cast<byte>(ControlMessage::endPayload)); break;
        case PayloadKind::table: payload.push_back(static_cast<byte>(ControlMessage::endTable)); break;
        case PayloadKind::data: payload.push_back(static_cast<byte>(ControlMessage::endDataAttribute)); break;
        case PayloadKind::ref: payload.push_back(static_cast<byte>(ControlMessage::endReferenceAttribute)); break;
        }
        return "";
    }

    // assumes that there is only one column loaded
    std::string sort() {
        ordering.clear();

        auto type = columnType(table, column);
        auto count = columnCount(table, column);
        ordering.reserve(count);

        for (std::uint64_t i = 0; i < count; i++)
            ordering.push_back(i);

        switch (type) {
        case AttributeKind::i8:        std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.i8 < data[r].data.i8; }); break;
        case AttributeKind::i16:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.i16 < data[r].data.i16; }); break;
        case AttributeKind::i32:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.i32 < data[r].data.i32; }); break;
        case AttributeKind::i64:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.i64 < data[r].data.i64; }); break;
        case AttributeKind::u8:        std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.u8 < data[r].data.u8; }); break;
        case AttributeKind::u16:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.u16 < data[r].data.u16; }); break;
        case AttributeKind::u32:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.u32 < data[r].data.u32; }); break;
        case AttributeKind::u64:       std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.u64 < data[r].data.u64; }); break;
        case AttributeKind::boolean:   std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.boolean < data[r].data.boolean; }); break;
        case AttributeKind::float_:    std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.float_ < data[r].data.float_; }); break;
        case AttributeKind::double_:   std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.double_ < data[r].data.double_; }); break;
        case AttributeKind::string:    std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.string < data[r].data.string; }); break;
        case AttributeKind::reference: std::sort(ordering.begin(), ordering.end(), [&](std::uint64_t l, std::uint64_t r){ return data[l].data.reference < data[r].data.reference; }); break;
        }
        return "";
    }

    std::string free() {
        data.resize(0);
        return "";
    }

    std::string execute(std::vector<Instruction> const& instructions) {
        clearState();

        std::uint64_t ic = 0;
        std::uint64_t n = instructions.size();

        while (ic < n) {
            auto& ins = instructions[ic];
            if (verbose) {
                std::cout << "Executing: ";
                print(ins);
            }
            switch (ins.kind) {
            case InstructionKind::createTable:
                abortIfFails(createTable(ins.data.createTable.name));
                ic++;
                break;
            case InstructionKind::createColumn:
                abortIfFails(createColumn(ins.data.createColumn.name, ins.data.createColumn.type));
                ic++;
                break;
            case InstructionKind::selectTable:               
                abortIfFails(selectTable(ins.data.selectTable.name));
                ic++;
                break;
            case InstructionKind::selectColumn:
                abortIfFails(selectColumn(ins.data.selectColumn.name));
                ic++;
                break;
            case InstructionKind::readColumn:
                abortIfFails(readColumn());
                ic++;
                break;
            case InstructionKind::appendColumn:
                abortIfFails(appendColumn(ins.data.appendColumn.attr));
                ic++;
                break;
            case InstructionKind::end:
                goto end;
            case InstructionKind::send:
                abortIfFails(send());
                ic++;
                break;
            case InstructionKind::open:
                abortIfFails(open(ins.data.open.kind));
                ic++;
                break;
            case InstructionKind::close:
                abortIfFails(close(ins.data.close.kind));
                ic++;
                break;
            case InstructionKind::sort:
                abortIfFails(sort());
                ic++;
                break;
            case InstructionKind::free:
                abortIfFails(free());
                ic++;
                break;
            }
        }

    end:
        std::ofstream f(dumpFile);
        for (auto&& b : payload)
            f << b;
        return "";
    }

};

std::vector<std::string> split(std::string const& s, char c) {
    std::vector<std::string> words;
    std::string buff;
    for (auto&& x : s)
        if (x == c && buff != "") {
            words.push_back(buff);
            buff.clear();
        }
        else {
            buff += x;
        }
    if (!buff.empty())
        words.push_back(buff);
    return words;
}

AttributeKind parseType(std::string const& word) {
    if (word == "i8") return AttributeKind::i8;
    else if (word == "i16") return AttributeKind::i16;
    else if (word == "i32") return AttributeKind::i32;
    else if (word == "i64") return AttributeKind::i64;
    else if (word == "u8") return AttributeKind::u8;
    else if (word == "u16") return AttributeKind::u16;
    else if (word == "u32") return AttributeKind::u32;
    else if (word == "u64") return AttributeKind::u64;
    else if (word == "string") return AttributeKind::string;
    else if (word == "bool") return AttributeKind::boolean;
    else if (word == "float") return AttributeKind::float_;
    else if (word == "double") return AttributeKind::double_;
    else throw std::runtime_error("Imma reading bullshit here");
}

void parseAttr(std::string const& word, Attribute& attr) {
    if (word == "true") {
        attr.data.boolean = true;
        attr.kind = AttributeKind::boolean;
    }
    else if (word == "false") {
        attr.data.boolean = false;
        attr.kind = AttributeKind::boolean;
    }
    else if (word.size() > 1 && word[0] == '"' && word[word.size() - 1] == '"') {
        attr.data.string = word.substr(1, word.size() - 2);
        attr.kind = AttributeKind::string;
    }
    else if (auto i = word.find_first_of('.'); i != static_cast<std::size_t>(-1) && i == word.find_last_of('.')) {
        attr.data.double_ = std::stod(word);
        attr.kind = AttributeKind::double_;
    }
    else {
        attr.data.u64 = std::stol(word);
        attr.kind = AttributeKind::u64;
    }
}

PayloadKind parsePayloadKind(std::string const& word) {
    if (word == "payload") return PayloadKind::payload;
    else if (word == "table") return PayloadKind::table;
    else if (word == "data") return PayloadKind::data;
    else if (word == "ref") return PayloadKind::ref;
    else throw std::runtime_error("Imma reading bullshit here");
}

std::vector<Instruction> loadInstructions(std::string const& filename) {
    std::ifstream file(filename);

    std::vector<Instruction> instructions;

    std::string line;

    while (std::getline(file, line)) {
        auto words = split(line, ' ');
        auto n = words.size();
        if (n > 0) {
            if (words[0].starts_with("//"))
                continue;
            else if (words[0] == "select") {
                if (n == 3) {
                    if (words[1] == "table") {
                        Instruction ins;
                        ins.kind = InstructionKind::selectTable;
                        ins.data.selectTable.name = words[2];
                        instructions.push_back(ins);
                        continue;
                    }
                    else if (words[1] == "column") {
                        Instruction ins;
                        ins.kind = InstructionKind::selectColumn;
                        ins.data.selectColumn.name = words[2];
                        instructions.push_back(ins);
                        continue;
                    }
                }
            }  
            else if (words[0] == "create") {
                if (words[1] == "table") {
                    if (n == 3) {
                        Instruction ins;
                        ins.kind = InstructionKind::createTable;
                        ins.data.createTable.name = words[2];
                        instructions.push_back(ins);                        
                        continue;
                    }
                }
                else if (words[1] == "column") {
                    if (n == 4) {
                        Instruction ins;
                        ins.kind = InstructionKind::createColumn;
                        ins.data.createColumn.name = words[2];
                        ins.data.createColumn.type=parseType(words[3]);
                        instructions.push_back(ins);
                        continue;
                    }
                }
            }
            else if (words[0] == "read") {
                Instruction ins;
                ins.kind = InstructionKind::readColumn;
                ins.data.readColumn = {};
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "append") {
                if (n == 2) {
                    Instruction ins;
                    ins.kind = InstructionKind::appendColumn;
                    parseAttr(words[1], ins.data.appendColumn.attr);
                    instructions.push_back(ins);
                    continue;
                }
            }
            else if (words[0] == "end") {
                Instruction ins;
                ins.kind = InstructionKind::end;
                ins.data.end = {};
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "send") {
                Instruction ins;
                ins.kind = InstructionKind::send;
                ins.data.send = {};
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "open" && n == 2) {
                Instruction ins;
                ins.kind = InstructionKind::open;
                ins.data.open.kind = parsePayloadKind(words[1]);
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "close" && n == 2) {
                Instruction ins;
                ins.kind = InstructionKind::close;
                ins.data.close.kind = parsePayloadKind(words[1]);
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "sort") {
                Instruction ins;
                ins.kind = InstructionKind::sort;
                ins.data.sort = {};
                instructions.push_back(ins);
                continue;
            }
            else if (words[0] == "free") {
                Instruction ins;
                ins.kind = InstructionKind::free;
                ins.data.free = {};
                instructions.push_back(ins);
                continue;
            }
            throw std::runtime_error("Imma reading bullshit here");
        }
    }

    return instructions;
}



int main(int argc, char** argv) {
    if (argc < 2) return 1;
    
    std::string filename = argv[1];
  
    if (argc == 3) { //} && strcmp(argv[2], "--verbose") == 1) {
        verbose = true;
    }

    std::cout << "Loading file: " << filename << std::endl;

    DataBase db("out.hex");

    auto instructions = loadInstructions(filename);

    std::cout << "Loaded Instructions (" << instructions.size() << ")" << std::endl;

    auto opt = db.execute(instructions);

    if (!opt.empty())
        std::cerr << "ERROR: " << opt << std::endl;
}
