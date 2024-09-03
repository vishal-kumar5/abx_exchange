#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace std;

const char* SERVER_IP = "127.0.0.1"; 
const int SERVER_PORT = 3000;

struct Packet {
    string symbol;
    char buySellIndicator;
    uint32_t quantity;
    uint32_t price;
    uint32_t sequence;

    json toJson() const {
        return {
            {"symbol", symbol},
            {"buySellIndicator", string(1, buySellIndicator)},
            {"quantity", quantity},
            {"price", price},
            {"sequence", sequence}
        };
    }
};

int connectToServer() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        cout << "Error creating socket" << endl;
        return -1;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);

    if (inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr) <= 0) {
        cout << "Invalid address" << endl;
        return -1;
    }

    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        cout << "Connection Failed" << endl;
        return -1;
    }

    return sockfd;
}

void sendStreamAllPacketsRequest(int sockfd) {
    bool request[2] = {1, 0}; 
    send(sockfd, request, sizeof(request), 0);
}


vector<Packet> receivePackets(int sockfd) {
    vector<Packet> packets;
    uint8_t buffer[17]; 

    while (true) {
        int bytesRead = recv(sockfd, buffer, sizeof(buffer), 0);
        if (bytesRead == 0) break; 
        if (bytesRead < 0) {
            cout << "Error receiving data" << endl;
            break;
        }

        Packet packet;
        packet.symbol = string((char*)&buffer[0], 4);
        packet.buySellIndicator = buffer[4];
        packet.quantity = ntohl(*(uint32_t*)&buffer[5]);
        packet.price = ntohl(*(uint32_t*)&buffer[9]);
        packet.sequence = ntohl(*(uint32_t*)&buffer[13]);

        packets.push_back(packet);
    }

    return packets;
}


void requestMissingPackets(int sockfd, const vector<int>& missingSequences, vector<Packet>& packets) {

    for (int i=0;i<missingSequences.size();i++) {

        uint8_t request[2] = {2, (uint8_t)missingSequences[i]};
        send(sockfd, request, sizeof(request), 0);
        uint8_t buffer[17];
        int bytesRead = recv(sockfd, buffer, sizeof(buffer), 0);
        if(bytesRead<=0){
            cout<<"error in recieving packets"<<endl;
            return;
        }
        Packet packet;
        packet.symbol = string((char*)&buffer[0], 4);
        packet.buySellIndicator = buffer[4];
        packet.quantity = ntohl(*(uint32_t*)&buffer[5]);
        packet.price = ntohl(*(uint32_t*)&buffer[9]);
        packet.sequence = ntohl(*(uint32_t*)&buffer[13]);
        packets.push_back(packet);
        
    }

    sort(packets.begin(), packets.end(), [](const Packet& a, const Packet& b) {
        return a.sequence < b.sequence;
    });
}


void writePacketsToJsonFile(const vector<Packet>& packets, const string& filename) {
    json jsonArray = json::array();
    for (const auto& packet : packets) {
        jsonArray.push_back(packet.toJson());
    }

    ofstream outFile(filename);
    outFile << jsonArray.dump(4); 
    outFile.close();
}


int main() {

    int sockfd = connectToServer();
    if (sockfd < 0) {
        cout << "Failed to connect to the server!" << endl;
        return 1;
    }

    sendStreamAllPacketsRequest(sockfd);
    
    vector<Packet> packets = receivePackets(sockfd);

    vector<int> missingSequences;
    for (int i = 1; i < packets.size(); i++) {
        int expectedSeq = packets[i - 1].sequence + 1;
        if (packets[i].sequence != expectedSeq) {
            for (int seq = expectedSeq; seq < packets[i].sequence; seq++) {
                missingSequences.push_back(seq);
            }
        }
    }

    requestMissingPackets(sockfd, missingSequences, packets);

    writePacketsToJsonFile(packets, "output.json");

    close(sockfd);

    return 0;
}

