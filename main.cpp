#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <exception>
#include <filesystem>

using byte = std::uint8_t;
using bytes = std::vector<byte>;

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

struct AttributeBaseView {
    AttributeKind kind;
    std::string name;
    std::uint64_t count;
};

struct AttributeView : AttributeBaseView {
    void* data;
};

struct AttributeFileView : AttributeBaseView {
};

struct TableView {
    std::string name;
    std::vector<AttributeView> attributes;
};

struct PayloadView {
    std::vector<TableView> tables;
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

template <typename T>
void serialize(T* const& data, std::uint64_t count, bytes& v) {
    v.reserve(v.size() + count * sizeof(T));
    for (std::uint64_t i = 0; i < count; i++)
        serialize(data[i], v);
}

void serialize(std::string const& s, bytes& v) {
    serialize(s.size(), v);
    std::copy(s.begin(), s.end(), std::back_inserter(v));
}

void serialize(AttributeKind const& k, bytes& v) {
    serialize(static_cast<std::uint8_t>(k), v);
}

template <typename F>
void serialize(AttributeBaseView const& a, bytes& v, F&& f) {
    bool isReference = a.kind == AttributeKind::reference;

    serialize(isReference ? ControlMessage::startReferenceAttribute : ControlMessage::startDataAttribute, v);

    serialize(a.name, v);

    if (!isReference)
        serialize(a.kind, v);

    serialize(a.count, v);

    f();

    serialize(isReference ? ControlMessage::endReferenceAttribute : ControlMessage::endDataAttribute, v);
}

void serialize(AttributeView const& a, bytes& v) {
    serialize(static_cast<AttributeBaseView>(a), v, [&](){
        switch (a.kind)
        {
            case AttributeKind::i8:
                serialize(static_cast<std::int8_t*>(a.data), a.count, v);
                break;
            case AttributeKind::i16:
                serialize(static_cast<std::int16_t*>(a.data), a.count, v);
                break;
            case AttributeKind::i32:
                serialize(static_cast<std::int32_t*>(a.data), a.count, v);
                break;
            case AttributeKind::i64:
                serialize(static_cast<std::int64_t*>(a.data), a.count, v);
                break;
            case AttributeKind::u8:
                serialize(static_cast<std::uint8_t*>(a.data), a.count, v);
                break;
            case AttributeKind::u16:
                serialize(static_cast<std::uint16_t*>(a.data), a.count, v);
                break;
            case AttributeKind::u32:
                serialize(static_cast<std::uint32_t*>(a.data), a.count, v);
                break;
            case AttributeKind::u64:
                serialize(static_cast<std::uint64_t*>(a.data), a.count, v);
                break;
            case AttributeKind::string:
                serialize(static_cast<std::string*>(a.data), a.count, v);
                break;
            case AttributeKind::boolean:
                serialize(static_cast<bool*>(a.data), a.count, v);
                break;
            case AttributeKind::float_:
                serialize(static_cast<float*>(a.data), a.count, v);
                break;
            case AttributeKind::double_:
                serialize(static_cast<double*>(a.data), a.count, v);
                break;
            case AttributeKind::reference:
                serialize(static_cast<int*>(a.data), a.count, v);
                break;
        }
    });
}

void serialize(Attribute const& a, bytes& v) {
    switch (a.kind)
    {
        case AttributeKind::i8:
            serialize(a.data.i8, v);
            break;
        case AttributeKind::i16:
            serialize(a.data.i16, v);
            break;
        case AttributeKind::i32:
            serialize(a.data.i32, v);
            break;
        case AttributeKind::i64:
            serialize(a.data.i64, v);
            break;
        case AttributeKind::u8:
            serialize(a.data.u8, v);
            break;
        case AttributeKind::u16:
            serialize(a.data.u16, v);
            break;
        case AttributeKind::u32:
            serialize(a.data.u32, v);
            break;
        case AttributeKind::u64:
            serialize(a.data.u64, v);
            break;
        case AttributeKind::string:
            serialize(a.data.string, v);
            break;
        case AttributeKind::boolean:
            serialize(a.data.boolean, v);
            break;
        case AttributeKind::float_:
            serialize(a.data.float_, v);
            break;
        case AttributeKind::double_:
            serialize(a.data.double_, v);
            break;
        case AttributeKind::reference:
            throw std::runtime_error("Internal error: I think");
    }
}

void serialize(AttributeFileView const& a, FILE* fd, std::uint64_t size, bytes& v) {
    serialize(static_cast<AttributeBaseView>(a), v, [&](){
        v.reserve(v.size() + size);
        std::uint8_t* p = new std::uint8_t[size];
        fread(p, 1, size, fd);
        std::copy(p, p + size, std::back_inserter(v));
        delete[] p;
    });
}

void serialize(TableView const& t, bytes& v) {
    v.push_back(ControlMessage::startTable);

    serialize(t.name, v);

    for (auto&& a : t.attributes)
        serialize(a, v);

    v.push_back(ControlMessage::endTable);
}

void serialize(PayloadView const& p, bytes& v) {
    v.push_back(ControlMessage::startPayload);

    for (auto&& t : p.tables)
        serialize(t, v);

    v.push_back(ControlMessage::endPayload);
}

enum class InstructionKind {
    selectTable,
    createTable,
    createColumn,
    selectColumn,
    readColumn,
    appendColumn,
    end,
    loadCount,
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
        struct {} loadCount;

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
        case InstructionKind::loadCount:
            data.loadCount = i.data.loadCount;
            break;
        }
    }
};

struct ColumnHandle {
private:
    std::string filename;
    std::string name;
    AttributeKind type;
    std::uint64_t count;

    long filesize(FILE* fd) {
        fseek(fd, 0, SEEK_END);

        auto size = ftell(fd);

        fseek(fd, 0, SEEK_SET);

        return size;
    }

public:
    ColumnHandle() = default;
    ColumnHandle(std::string const& table, std::string const& name, AttributeKind const& type) : filename(table + "/" + name), name(name), type(type), count(0) {
        if (!std::filesystem::exists(filename)) {
            std::ofstream f(filename);
            f.flush();
        }
    }

    std::string read(bytes& v) {
        std::cout << "Reading: " << filename << std::endl;

        FILE* fd = fopen(filename.c_str(), "rb");

        if (fd == nullptr)
            throw std::runtime_error("bad file");

        long size = filesize(fd);

        AttributeFileView attr;
        attr.kind = type;
        attr.count = count;
        attr.name = name;

        serialize(attr, fd, size, v);

        fclose(fd);

        return "";
    }

    std::string append(Attribute const& attr) {
        if (attr.kind != type)
            return "Cannot append to " + name + " type mismatch";

        FILE* fd = fopen(filename.c_str(), "ab");

        bytes b;
        serialize(attr, b);

        fwrite(b.data(), 1, b.size(), fd);

        fclose(fd);

        count++;

        return "";
    }
    
    std::string update() {
        throw std::runtime_error("TODO");
    }
    
    std::string delete_() {
        throw std::runtime_error("TODO");
    }

    std::string loadCount() {
        FILE* fd = fopen(filename.c_str(), "rb");

        if (fd == nullptr)
            return "bad file";

        count = filesize(fd) / attributeSize(type);

        return "";
    }
};

struct TableHandle {
private:
    std::string name;
    std::unordered_map<std::string, ColumnHandle> columns;
    ColumnHandle* selectedHandle;
public:
    TableHandle() = default;
    TableHandle(std::string const& name) : name(name) {
        if (!std::filesystem::exists(name)) {
            std::filesystem::create_directory(name);
        }
        else if (std::filesystem::status(name).type() != std::filesystem::file_type::directory) {
            throw std::runtime_error("file: " + name + " exists but is not a directory");
        }
    }

    void clearState() {
        selectedHandle = nullptr;
    }

    std::string createColumn(std::string const& name, AttributeKind const& type) {
        if (columns.find(name) != columns.end())
            return "On Table: " + this->name + " column: " + name + " already exists";

        columns[name] = ColumnHandle(this->name, name, type);
        selectedHandle = &columns[name];

        return "";
    } 

    std::string selectColumn(std::string const& name) {
        if (columns.find(name) == columns.end())
            return "Cannot select an non existent column named: " + name + " on table: " + this->name;
        selectedHandle = &columns[name];
        return "";
    }

    std::string readColumn(bytes& v) {
        if (selectedHandle == nullptr)
            return "Cannot read column when table " + name + " is selected but no column is selected";
        return selectedHandle->read(v);
    } 

    std::string appendColumn(Attribute const& attr) {
        if (selectedHandle == nullptr)
            return "Cannot append column when table " + name + " is selected but no column is selected";
        return selectedHandle->append(attr);
    }

    std::string loadCount() {
        if (selectedHandle == nullptr)
            return "Cannot load count when table " + name + " is selected but no column is selected";
        return selectedHandle->loadCount();
    }
};

#define abortIfFails(x) if (auto e = x; !e.empty()) return std::make_pair(bytes{}, e)

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
    case InstructionKind::loadCount:
        return static_cast<void>(std::cout << "load count" << std::endl);
    }
}

struct DataBase {
private:
    std::unordered_map<std::string, TableHandle> tables;
    TableHandle* selectedHandle;
public:

    void clearState() {
        for (auto&& [k, v] : tables)
            v.clearState();
        selectedHandle = nullptr;
    }

    std::string createTable(std::string const& name) {
        if (tables.find(name) != tables.end())
            return "Table: " + name + " already exists";

        tables[name] = TableHandle(name);
        selectedHandle = &tables[name];

        return "";
    }

    std::string createColumn(std::string const& name, AttributeKind const& type) {
        if (selectedHandle == nullptr)
            return "Cannot create column when no table is selected";
        return selectedHandle->createColumn(name, type);
    }

    std::string selectTable(std::string const& name) {
        if (tables.find(name) == tables.end())
            return "Cannot select an non existent table named: " + name;
        selectedHandle = &tables[name];
        return "";
    }

    std::string selectColumn(std::string const& name) {
        if (selectedHandle == nullptr)
            return "A table is not selected to select to a column";
        return selectedHandle->selectColumn(name);
    }  

    std::string readColumn(bytes& v) {
        if (selectedHandle == nullptr)
            return "A table is not selected to read from a column";
        return selectedHandle->readColumn(v);
    }

    std::string appendColumn(Attribute const& attr) {
        if (selectedHandle == nullptr)
            return "A table is not selected to append to a column";
        return selectedHandle->appendColumn(attr);
    }

    std::string loadCount() {
        if (selectedHandle == nullptr)
            return "A table is not selected to load count for a column";
        return selectedHandle->loadCount();
    }

    std::pair<bytes, std::string> execute(std::vector<Instruction> const& instructions) {
        clearState();

        std::uint64_t ic = 0;
        std::uint64_t n = instructions.size();

        bytes b;

        while (ic < n) {
            auto& ins = instructions[ic];
            std::cout << "Executing: ";
            print(ins);
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
                abortIfFails(readColumn(b));
                ic++;
                break;
            case InstructionKind::appendColumn:
                abortIfFails(appendColumn(ins.data.appendColumn.attr));
                ic++;
                break;
            case InstructionKind::loadCount:
                abortIfFails(loadCount());
                ic++;
                break;
            case InstructionKind::end:
                goto end;
            }
        }

    end:
        return std::make_pair(b, "");
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
        std::cout << i << std::endl;
        attr.data.double_ = std::stod(word);
        attr.kind = AttributeKind::double_;
    }
    else {
        attr.data.u64 = std::stol(word);
        attr.kind = AttributeKind::u64;
    }
}

std::vector<Instruction> loadInstructions(std::string const& filename) {
    std::ifstream file(filename);

    std::vector<Instruction> instructions;

    std::string line;

    while (std::getline(file, line)) {
        auto words = split(line, ' ');
        auto n = words.size();
        if (n > 0) {
            if (words[0] == "select") {
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
            else if (words[0] == "load" && n == 2 && words[1] == "count") {
                Instruction ins;
                ins.kind = InstructionKind::loadCount;
                ins.data.loadCount = {};
                instructions.push_back(ins);
                continue;
            }
            throw std::runtime_error("Imma reading bullshit here");
        }
    }

    return instructions;
}



int main(int argc, char** argv) {
    if (argc != 2) return 1;
    
    auto filename = argv[1];

    std::cout << "Loading file: " << filename << std::endl;

    DataBase db;

    auto instructions = loadInstructions(filename);

    std::cout << "Loaded Instructions (" << instructions.size() << ")" << std::endl;
    for (auto&& i : instructions)
        print(i);

    auto opt = db.execute(instructions);

    if (opt.second.empty()) {
        std::ofstream file("out.hex", std::ios::binary);
        for (auto& b : opt.first)
            file << b;
    } 
    else {
        std::cerr << opt.second << std::endl;
    }
}
