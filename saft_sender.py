import os
import datetime
import subprocess
import xml.etree.ElementTree as ET
import psycopg2
from prettytable import PrettyTable

# --------------------------------------- DIRECTORY VARIABLES --------------------------------------- #
INPUT_SAFT = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender\input_saft'
OUTPUT_SAFT = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender\output_saft'
OUTPUT_XML = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender\output_xml'
JAR_FILE = r'C:\Users\Frederico\Desktop\Frederico_Gago\Confere\Programas\saft_sender' \
           r'\jar_file\FACTEMICLI-2.5.16-9655-cmdClient.jar'

# --------------------------------------- CONSTANT VARIABLES --------------------------------------- #
DB_PASS = os.environ['DB_PASS']
ERROR_LIST = []
SENT_LIST = []


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
        """
        Search in xml file for the nif of the company
        :return: nif of the company file
        """
        try:
            root = ET.parse(self.path).getroot()
        except Exception as error:
            return str(error)
        else:
            for xml in root.iter("{urn:OECD:StandardAuditFile-Tax:PT_1.04_01}TaxRegistrationNumber"):
                nif = xml.text

            return nif

    def query_db(self):
        """
        Query Data Base for password and company id
        :return: Tuple of client_id and password
        """
        conn = psycopg2.connect(host='localhost', database='confere', user='postgres', password=DB_PASS)
        cur = conn.cursor()
        cur.execute(
            'SELECT c.client_id, f.password '
            'FROM companies AS c '
            'JOIN financas AS f '
            'ON c.client_id = f.client_id '
            'WHERE c.nif = (%s)',
            (self.nif,)
        )
        result = cur.fetchone()
        conn.close()
        return result

    def send_saft(self):
        """
        First Validates SAFT and checks error then send them
        :return:
        """
        language, file_reader, nif, password, year, month, operation, \
            input_param, output_param = "java", "-jar", "-n", "-p", "-a", "-m", "-op", "-i", "-o"

        type_operation = "validar"

        # Development Mode
        test = '-t' if development_mode else ''


        # Handle multiple outputs of the same company
        list_output_dir = os.listdir(OUTPUT_SAFT)
        string_output = ' '.join(list_output_dir)
        already_there = string_output.count(str(self.company_id))
        output_path = os.path.join(
            OUTPUT_SAFT,
            f'{self.company_id} - SAFT{"" if already_there == 0 else " Loja " + str(already_there + 1)}'
            f' {self.month}-{self.year}'
        )

        # First Validate Second Send
        for _ in range(2):
            line_commands = [language, file_reader, JAR_FILE, nif, str(self.nif), password,
                             str(self.password), year, self.year, month, self.month,
                             operation, type_operation, input_param, self.path, test]

            # OFF Development Mode
            if test == '':
                line_commands.pop()

            # In validation don't create output file
            if type_operation == 'enviar':
                line_commands = line_commands + [output_param, output_path]

            proc1 = subprocess.run(line_commands, capture_output=True)

            # Check Error & Warnings
            if type_operation == 'validar':
                print(f'Validating SAFT: {self.company_id} - {self.path}')

                stdout = proc1.stdout.decode('ISO-8859-1')
                if stdout.find('<errors>') != -1:
                    response_code = stdout[stdout.find('<response code='):stdout.find('</response>') + 11]
                    self.get_error(response_code, self.company_id)
                    return

                print(f'No errors found.\nSending SAFT...')

            type_operation = 'enviar'

        # Move xml files
        self.move_xml_files()

    def move_xml_files(self):
        """
        Move original xml file and resume file to a directory that has the name of the client
        :return:
        """
        final_file_destination = self.get_directory()
        # Move XML files that here sent to new directory
        # Move Original file
        original_file_to = os.path.join(final_file_destination, self.name_file)
        os.rename(self.path, original_file_to)

        # Move Resume file
        resume_file = f'{self.name_file[:-4]}.resumido.xml'
        resume_file_path = os.path.join(INPUT_SAFT, resume_file)
        resume_file_to = os.path.join(final_file_destination, resume_file)
        os.rename(resume_file_path, resume_file_to)

    def get_directory(self):
        """

        :return: The path of the directory to move the xml files
        """
        dir_list = os.listdir(OUTPUT_XML)
        directory_name = f'{self.company_id} - SAFT {self.month}-{self.year}'

        create_dir = os.path.join(OUTPUT_XML, directory_name)

        if directory_name in dir_list:
            return create_dir
        else:
            os.mkdir(create_dir)
            return create_dir

    def __repr__(self):
        return f'{self.company_id} - {self.nif} & {self.password}'

    def get_error(self, error, client=None):
        """
        If error exists it ill write a txt file with the error in error directory
        :param error:
        :param client:
        """
        self.error = True
        client = str(client) + " " if client else ""
        with open(f'error_saft/{client}{self.name_file} - Error.txt', 'w') as f:
            string = f'O SAFT situado em: {self.path}\nTeve o seguinte erro:\n{error}'
            f.write(string)

        print(f'{client}Error on saft {company_saft.name_file} check error file')
        ERROR_LIST.append([client, company_saft.name_file])


if __name__ == '__main__':
    # DEVELOPING MODE
    development_mode = True

    # Get corresponding date
    corresponding_date = month_in_reference()

    # Search for saft's to send
    saft_files_list = search_xml_files(INPUT_SAFT)

    for saft in saft_files_list:
        print(f'Handling saft: {saft}')
        # Instantiate company SAFT
        company_saft = SAFT(saft, corresponding_date)
        # Send Saft
        company_saft.send_saft()

        print('-'*200)

        # If no error
        if not company_saft.error:
            SENT_LIST.append([company_saft.company_id, company_saft.name_file])

    # Printing Result of the program in a Pretty Table
    show_results = PrettyTable(['id', 'Ficheiro', 'Status'])
    show_results.align = 'l'

    for error in ERROR_LIST:
        show_results.add_row([int(error[0]), error[1], 'Error'])

    for sent in SENT_LIST:
        show_results.add_row([int(sent[0]), sent[1], 'Sent'])

    result_string = show_results.get_string(sortby='id')
    print(result_string)

    # Saving in a file
    with open(f'./{corresponding_date} - log_saft_sender.txt', 'a') as f:
        f.write('-' * 60 + str(datetime.datetime.now()) + '-' * 60 + '\n')
        f.write(result_string + '\n\n')

