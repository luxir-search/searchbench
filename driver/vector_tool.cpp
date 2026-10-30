// Numeric preparation only. Timed HTTP requests use bench_replay.
#include <algorithm>
#include <atomic>
#include <bit>
#include <charconv>
#include <cmath>
#include <fstream>
#include <iostream>
#include <queue>
#include <thread>
#include <vector>

std::vector<float> read(const char* path, size_t n) {
  std::ifstream in(path, std::ios::binary);
  std::vector<float> v(n);
  if (!in.read(reinterpret_cast<char*>(v.data()), n * sizeof(float)))
    throw std::runtime_error("short vector file");
  for (float f : v) if (!std::isfinite(f)) throw std::runtime_error("nonfinite vector");
  return v;
}
int main(int argc, char** argv) try {
  if (std::endian::native != std::endian::little) throw std::runtime_error("requires little endian");
  if (argc < 5) throw std::runtime_error("encode FILE ROWS DIMS | truth DOCS ROWS DIMS QUERIES NQ K THREADS");
  size_t n = std::stoull(argv[3]), dims = std::stoull(argv[4]);
  if (!n || !dims) throw std::runtime_error("empty shape");
  if (std::string(argv[1]) == "encode") {
    std::ifstream in(argv[2], std::ios::binary);
    std::vector<float> row(dims);
    for (size_t i=0; i<n; ++i) {
      if (!in.read(reinterpret_cast<char*>(row.data()), dims*4)) throw std::runtime_error("short vector file");
      std::string line = "[";
      for (size_t d=0; d<dims; ++d) {
        if (!std::isfinite(row[d])) throw std::runtime_error("nonfinite vector");
        char buf[64]; auto r=std::to_chars(buf, buf+64, row[d]);
        if (r.ec != std::errc{}) throw std::runtime_error("float serialization");
        if (d) line += ',';
        if (row[d] == 0 && std::signbit(row[d])) line += "-0.0";
        else line.append(buf, r.ptr);
      }
      std::cout << line << "]\n";
    }
  } else if (std::string(argv[1]) == "truth" && (argc == 9 || argc == 10)) {
    size_t nq=std::stoull(argv[6]), k=std::stoull(argv[7]), nt=std::stoull(argv[8]);
    if (!nq || !k || k>n || !nt) throw std::runtime_error("invalid truth shape");
    std::string metric = argc == 10 ? argv[9] : "ip";
    if (metric != "ip" && metric != "l2" && metric != "cosine") throw std::runtime_error("invalid metric");
    auto docs=read(argv[2], n*dims), queries=read(argv[5], nq*dims);
    std::vector<double> dn(n, 1), qn(nq, 1);
    if (metric == "cosine") {
      auto norms = [dims](const auto& values, auto& norms) {
        for (size_t i=0; i<norms.size(); ++i) {
          double sum=0;
          for (size_t d=0; d<dims; ++d) sum += double(values[i*dims+d])*values[i*dims+d];
          if (!(sum > 0)) throw std::runtime_error("zero cosine vector");
          norms[i]=std::sqrt(sum);
        }
      };
      norms(docs,dn); norms(queries,qn);
    }
    using Hit=std::pair<double, int>; // larger score, then smaller ordinal
    std::vector<std::vector<Hit>> hits(nq);
    std::atomic<size_t> next{0};
    std::vector<std::thread> workers;
    for (size_t t=0; t<std::min(nt,nq); ++t) workers.emplace_back([&] {
      for (size_t q; (q=next++)<nq;) {
        std::priority_queue<Hit,std::vector<Hit>,std::greater<Hit>> heap;
        for (size_t i=0; i<n; ++i) {
          double score=0;
          if (metric == "l2") {
            for (size_t d=0; d<dims; ++d) {
              double delta=double(docs[i*dims+d])-double(queries[q*dims+d]);
              score -= delta*delta;
            }
          } else {
            for (size_t d=0; d<dims; ++d) score += double(docs[i*dims+d])*double(queries[q*dims+d]);
            if (metric == "cosine") score /= dn[i]*qn[q];
          }
          Hit hit{score,-int(i)};
          if (heap.size()<k) heap.push(hit);
          else if (hit>heap.top()) { heap.pop(); heap.push(hit); }
        }
        while (!heap.empty()) { hits[q].push_back(heap.top()); heap.pop(); }
        std::reverse(hits[q].begin(),hits[q].end());
      }
    });
    for (auto& w:workers) w.join();
    std::cout << '[';
    for (size_t q=0;q<nq;++q) {
      if(q) std::cout << ',';
      std::cout << '[';
      for(size_t j=0;j<k;++j) { if(j) std::cout << ','; std::cout << '"' << -hits[q][j].second << '"'; }
      std::cout << ']';
    }
    std::cout << "]\n";
  } else throw std::runtime_error("invalid mode/arguments");
} catch(const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
