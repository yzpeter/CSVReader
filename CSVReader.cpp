// CSVReader.cpp : contains main function, run this file
// Use OpenMP for multi-threading, total running time is around 1100 seconds on my laptop

#include <iostream>
#include <string>
#include <vector>
#include <omp.h>
#include <chrono>
#include <cmath>

using namespace std;

const int numThreads = 4;

void showTime(chrono::time_point<chrono::high_resolution_clock> start, string procedure) {
	// show codes running time
	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto dur = chrono::duration_cast<chrono::seconds>(end- start).count();
	cout << procedure << " took " << dur << " seconds" << endl;
}

int numLines(string filePath) {
	// count total number of lines, use C style fgets, 50% faster than C++ stdin
	auto st = chrono::high_resolution_clock::now();
	FILE* fp = fopen(filePath.c_str(), "r");
	char chline[100];
	fgets(chline, 100, fp);
	int num = 0;
	while (fgets(chline, 100, fp) != NULL)
		num++;
	fclose(fp);
	showTime(st, "Count number of lines");
	return num;
}

class TxnData {
private:
	double bidPriceSum;
	double askPriceSum;
	double bidPriceSqSum;
	double askPriceSqSum;
	double bidSizeSum;
	double bidSizeSqSum;
	double askSizeSum;
	double askSizeSqSum;
	double priceCrossSum;
	double sizeCrossSum;
	int numTxn;

public:
	TxnData(string filePath) {
		// using multi-thread to read from csv file, update statistics in real-time, space complexity O(1)
		double bpSum = 0.0, apSum = 0.0, bpSqSum = 0.0, apSqSum = 0.0, bsSum = 0.0, bsSqSum = 0.0, asSum = 0.0, asSqSum = 0.0, pcSum=0.0, scSum=0.0;
		int totalNumTxn = numLines(filePath);
		auto start = chrono::high_resolution_clock::now();
		FILE* fp = fopen(filePath.c_str(), "r");
		char chline[100];
		fgets(chline, 100, fp);
#pragma omp parallel for reduction(+:bpSum,apSum,bpSqSum,apSqSum,bsSum,bsSqSum,asSum,asSqSum,pcSum,scSum)
		for (int i = 0; i < totalNumTxn; i++) {
			char chline[100];
			fgets(chline, 100, fp);
			vector<double> txnInfo;
			string line(chline);
			size_t st = line.find_first_of(',', line.find_first_of(',') + 1) + 1;
			size_t ed = line.find_first_of(',', st);
			for (int j = 0; j < 4; j++) {
				txnInfo.emplace_back(stod(line.substr(st, ed - st)));
				st = ed + 1;
				ed = line.find_first_of(',', st);
			}
			bpSum += txnInfo[0], apSum += txnInfo[2], bpSqSum += txnInfo[0] * txnInfo[0], apSqSum += txnInfo[2] * txnInfo[2], pcSum += txnInfo[0] * txnInfo[2];
			bsSum += txnInfo[1], bsSqSum += txnInfo[1] * txnInfo[1], asSum += txnInfo[3], asSqSum += txnInfo[3] * txnInfo[3], scSum += txnInfo[1] * txnInfo[3];
		}
		fclose(fp);
		bidPriceSum = bpSum, bidPriceSqSum = bpSqSum, askPriceSum = apSum, askPriceSqSum = apSqSum, priceCrossSum = pcSum;
		bidSizeSum = bsSum, bidSizeSqSum = bsSqSum, askSizeSum = asSum, askSizeSqSum = asSqSum, sizeCrossSum = scSum;
		numTxn = totalNumTxn;
		showTime(start, "Read csv and update statistics");
	}

	void showStats() {
		// calculate and show statistics
		if (numTxn == 0) {
			cout << "There is no trade record" << endl;
			return;
		}
		double bidMean = bidPriceSum / numTxn;
		double askMean = askPriceSum / numTxn;
		double bidVar = bidPriceSqSum / (numTxn - 1) - bidMean * bidMean;
		double askVar = askPriceSqSum / (numTxn - 1) - askMean * askMean;
		double priceCov = priceCrossSum / (numTxn - 1) - bidMean * askMean;
		double priceCorr = priceCov / sqrt(bidVar*askVar);
		cout << "Bid price mean = " << bidMean << ", variance = " << bidVar << endl;
		cout << "Ask price mean = " << askMean << ", variance = " << askVar << endl;
		cout << "Bid-ask price covariance = " << priceCov << ", correlation = " << priceCorr << endl;
		cout << endl;
		double bidSizeMean = bidSizeSum / numTxn;
		double bidSizeVar = bidSizeSqSum / (numTxn - 1) - bidSizeMean * bidSizeMean;
		double askSizeMean = askSizeSum / numTxn;
		double askSizeVar = askSizeSqSum / (numTxn - 1) - askSizeMean * askSizeMean;
		double sizeCov = sizeCrossSum / (numTxn - 1) - bidSizeMean * askSizeMean;
		double sizeCorr = sizeCov / sqrt(bidSizeVar*askSizeVar);
		cout << "Bid size mean = " << bidSizeMean << ", variance = " << bidSizeVar << endl;
		cout << "Ask size mean = " << askSizeMean << ", variance = " << askSizeVar << endl;
		cout << "Bid-ask size covariance = " << sizeCov << ", correlation = " << sizeCorr << endl;
		return;
	}
};


int main()
{
	string path = "quote.csv";
	omp_set_num_threads(numThreads);
	TxnData data(path);
	data.showStats();
	return 0;
}
