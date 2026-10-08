// server.cpp
#include <cctype>
#include <string>

#include <fipc.hpp>

int main()
{
    auto listener = fipc::listener::listen("my_channel", 1 << 20);  // rings of 1 MiB each way
    if (!listener)
        return 1;
    auto conn = listener->accept();  // waits for a client
    if (!conn)
        return 1;
    std::string payload;
    auto request = conn->rpc_receive(payload);
    if (!request)
        return 1;
    for (char& c : payload)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return conn->rpc_respond(request->id, request->opcode, 0, payload) ? 0 : 1;
}
