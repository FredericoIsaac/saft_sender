import os
import datetime
import subprocess
import xml.etree.ElementTree as ET
import psycopg2

# --------------------------------------- DIRECTORY VARIABLES --------------------------------------- #
INPUT_SAFT = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender\input_saft'
OUTPUT_SAFT = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender\output_saft'
JAR_FILE = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender' \
           r'\jar_file\FACTEMICLI-2.5.16-9655-cmdClient.jar'

# --------------------------------------- CONSTANT VARIABLES --------------------------------------- #
DB_PASS = os.environ['DB_PASS']


# --------------------------------------- CORRESPONDING DATE --------------------------------------- #
def month_in_reference():
    """
    :return a String of the corresponding Month and Year of SAFT
    Example: current month 1 (January) of 2021 returns: 12-2020
    """
    months = [n for n in range(1, 13)]
    current_date = datetime.date.today()
    current_month = current_date.timetuple()[1]
    last_month = months[current_month - 2]
    current_year = current_date.timetuple()[0]

    if last_month == 12:
        curr_year = current_year - 1
    else:
        curr_year = current_year

    return f'{str(last_month).zfill(2)}-{curr_year}'


# --------------------------------------- SEARCH SAFT'S --------------------------------------- #
def search_xml_files(path_to_search):
    """
    Search in the path given xml files and create a list of them
    :param path_to_search: a path to the dir to search
    :return: A list of xml path files in param given
    """
    return [os.path.join(path_to_search, file) for file in os.listdir(path_to_search) if file.endswith('.xml')]


# --------------------------------------- SAFT CLASS --------------------------------------- #
class SAFT:
    def __init__(self, path_file, date):
        self.path = path_file
        self.name_file = os.path.basename(path_file)
        self.month = date.split('-')[0].strip()
        self.year = date.split('-')[1].strip()
        self.error = False

        self.nif = self.get_nif()
        # If no error parsing xml to get nif
        if self.nif.isnumeric():
            query = self.query_db()
            self.company_id = query[0]
            self.password = query[1]
        else:
            self.get_error(self.nif)

    def get_nif(self):
        try:
            root = ET.parse(self.path).getroot()
        except Exception as error:
            return str(error)
        else:
            for xml in root.iter("{urn:OECD:StandardAuditFile-Tax:PT_1.04_01}TaxRegistrationNumber"):
                nif = xml.text

            return nif

    def query_db(self):
        conn = psycopg2.connect(host='localhost', database='senhas', user='postgres', password=DB_PASS)
        cur = conn.cursor()
        cur.execute(
            'SELECT c.client_id, f.password '
            'FROM companies AS c '
            'JOIN financas AS f '
            'ON c.client_id = f.client_id '
            'WHERE c.nif = (%s)',
            (self.nif,)
        )
        return cur.fetchone()

    def send_saft(self):
        language, file_reader, nif, password, year, month, operation, \
            input_param, output_param = "java", "-jar", "-n", "-p", "-a", "-m", "-op", "-i", "-o"

        # TODO 1. Ver com faço o flow, 1º validar, caso haja erro escrever o erro para um pasta de erros, 2º enviar caso esteja tudo bem
        # 40432 tem erro e 10195 tem erro por estar sem faturação
        type_operation = "validar"  # "enviar"
        test = "-t"
        output_path = os.path.join(OUTPUT_SAFT, f'{self.company_id} - SAFT {self.month}-{self.year}')

        proc1 = subprocess.run(
            [language, file_reader, JAR_FILE, nif, str(self.nif), password, str(self.password), year, self.year,
             month, self.month, operation, type_operation, input_param,
             self.path, output_param, output_path, test], capture_output=True)

        if proc1.stderr:
            print(f'Error in {self.company_id}:\n{self.path}:\n', proc1.stderr.decode("ISO-8859-1"))

    def __repr__(self):
        return f'{self.company_id} - {self.nif} & {self.password}'

    def get_error(self, error):
        self.error = True
        with open(f'error_saft/{self.name_file} - Error.txt', 'w') as f:
            string = f'O SAFT situado em: {self.path}\nTeve o seguinte erro:\n{error}'
            f.write(string)


if __name__ == '__main__':
    # Get corresponding date
    corresponding_date = month_in_reference()

    # Search for saft's to send
    saft_files_list = search_xml_files(INPUT_SAFT)

    for saft in saft_files_list:
        # Instantiate company SAFT
        company_saft = SAFT(saft, corresponding_date)

        # If no error parsing xml
        if not company_saft.error:
            # Send Saft
            company_saft.send_saft()
            print(f'Sent saft of {company_saft.company_id}')
        else:
            print(f'Error on saft {company_saft.name_file} check error file')
