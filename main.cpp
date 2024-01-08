#include <iostream>
#include <vector>
#include <variant>
#include <random>

constexpr size_t kBlockSize = 2048;

using Attribute = std::variant<size_t, double, std::string>;




std::vector<std::vector<Attribute>> GenerateTable() {
    std::vector<std::vector<Attribute>> table(5e7);
    return table;
}

int main() {

}