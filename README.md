# Zookeeper Sync

<a name="readme-top"></a>

<!-- ABOUT THE PROJECT -->
## About The Project

The Zookeeper Sync Tool is a Python-based utility designed to synchronize Zookeeper nodes between two clusters. It ensures that the destination cluster mirrors the source cluster, including creating, updating, and deleting nodes as necessary.

### Built With

* [Kazoo](https://kazoo.readthedocs.io/)
* [Loguru](https://loguru.readthedocs.io/)
* [Pydantic](https://docs.pydantic.dev/)


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running, follow these steps.

### Prerequisites

* Python 3.8+
* pip

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/sphr2k/zksync.git
   ```
2. Navigate to the project directory:
   ```sh
   cd zksync
   ```
3. Install the required dependencies:
   ```sh
   pip install -r requirements.txt
   ```
4. Configure the `.env` file with your Zookeeper cluster details.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- USAGE EXAMPLES -->
## Usage

Run the tool to start synchronizing clusters:

```sh
python zksync.py
```

Make sure to configure the `SOURCE_HOSTS` and `DEST_HOSTS` in your `.env` file.


