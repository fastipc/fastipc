// server.cpp
#include <cctype>
#include <string>

#include <fipc.hpp>

int main()
{
    auto listener = fipc::stream_create("my_channel", 1 << 20);  // rings of 1 MiB each way
    if (!listener)
        return 1;
    auto conn = listener->accept();  // waits for a client
    if (!conn)
        return 1;
    std::string message;
    if (!conn->receive(message))
        return 1;
    for (char& c : message)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return conn->send(message) ? 0 : 1;
}
