"""Main module."""
import csv, json, pandas as pd
import os, sys, requests, datetime, time
import zipfile, io
import lxml.html as lhtml
import lxml.html.clean as lhtmlclean
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


class QualClient:
    """
    QualClient is a python wrapper the provides convenient access to data
    exports directly from Qualtrics into Pandas for further manipulation.
    
    The client in intiated with an API Token, and API URL
    It provides 3 Primary functions-
    
    QualClient.pull_survey_meta():
        Pulls down a complete list of your surveys and addtional parameters
        such as isActive, Creation Date, Mod Date, Name, and IDs
        
    QualClient.pull_definition(survey_id):
        survey_id : str
        Takes the supplied survey_id and returns a df with the
        survey's defintion info, which identifies things like the 
        questions asked, question text, question order, and IDs
        
    QualClient.pull_results(survey_id):
        survey_id : str
        Take the supplied survey_id and returns a df of all of the responses
        to the survey, with both the raw text and encoding of the response.
        This functionalty actually downloads and unzips files from Qualtrics, so be
        aware that it might take a moment to return the finalized data.
        DF takes the shape of a long table with one response per row.
        
    Example Usage:
    client = QualClient(API_Token, API_url)
    
    definitions = client.survey(survey_id)
    
    print(definitions.head())
    """
    def __init__(self, api_token, api_url):
        self.api_token = api_token
        self.headers = {
            'x-api-token': self.api_token,
            'content-type': "application/json",
            'cache-control': "no-cache"
        }
        self.api_url = api_url
        self.survey_url = self.api_url + 'surveys/'
        self.definition_url = self.api_url + 'survey-definitions/'
        self.response_url = self.api_url + 'responseexports/'
        self.failed_responses = ["cancelled", "failed"]

    def pull_survey_meta(self):
        arrQualtricsSurveys = []
        arrSurveyName = []
        arrSurveyActive = []
        arrCreation = []
        arrMod = []

        def GetQualtricsSurveys(qualtricsSurveysURL):
            response = requests.get(url=qualtricsSurveysURL,
                                    headers=self.headers)
            jsonResponse = response.json()
            nextPage = jsonResponse['result']['nextPage']

            arrQualtricsSurveys.extend(
                [srv['id'] for srv in jsonResponse['result']['elements']])
            arrSurveyName.extend(
                [srv['name'] for srv in jsonResponse['result']['elements']])
            arrSurveyActive.extend([
                srv['isActive'] for srv in jsonResponse['result']['elements']
            ])
            arrCreation.extend([
                srv['creationDate']
                for srv in jsonResponse['result']['elements']
            ])
            arrMod.extend([
                srv['lastModified']
                for srv in jsonResponse['result']['elements']
            ])

            #Contains nextPage
            if (nextPage is not None):
                GetQualtricsSurveys(nextPage)

        GetQualtricsSurveys(self.survey_url)

        df = pd.DataFrame({
            'SurveyID': arrQualtricsSurveys,
            'Survey_Name': arrSurveyName,
            'IsActive': arrSurveyActive,
            'Created': arrCreation,
            'LastModified': arrMod
        })
        return df

    def pull_definition(self, survey_id):

        response = json.loads(
            requests.get(
                url=self.definition_url + survey_id,
                headers=self.headers).content.decode("utf-8"))['result']

        question = pd.json_normalize(response['Questions']).melt()

        flow = pd.json_normalize(response['SurveyFlow']['Flow'])
        if ("EmbeddedData" in flow.columns or "Flow" in flow.columns):
            flow.rename(columns={
                'ID': 'BlockID',
                'Type': 'FlowType'
            },
                        inplace=True)
            if not 'BlockID' in flow.columns:
                flow['BlockID'] = ""
            flow = flow[[
                'EmbeddedData', 'FlowID', 'BlockID', 'Flow', 'FlowType'
            ]].reset_index()
            flow.rename(columns={'index': 'FlowSort'}, inplace=True)

            flow_block = flow[(
                flow.EmbeddedData.isnull() == True)].EmbeddedData.apply(
                    pd.Series).merge(
                        flow, right_index=True,
                        left_index=True).drop(["EmbeddedData"], axis=1).melt(
                            id_vars=[
                                'FlowSort', 'FlowID', 'BlockID', 'FlowType'
                            ],
                            value_name="EmbeddedData")

            embed = flow[(
                flow.EmbeddedData.isnull() == False)].EmbeddedData.apply(
                    pd.Series).merge(
                        flow, right_index=True,
                        left_index=True).drop(["EmbeddedData"], axis=1).melt(
                            id_vars=[
                                'FlowSort', 'FlowID', 'BlockID', 'FlowType'
                            ],
                            value_name="EmbeddedData")
            embed = embed.EmbeddedData.apply(pd.Series).merge(
                embed, right_index=True,
                left_index=True).drop(["EmbeddedData"],
                                      axis=1).dropna(subset=['Field', 'Type'])
            embed = embed[[
                'FlowSort', 'FlowID', 'BlockID', 'FlowType', 'Field', 'Type',
                'Value'
            ]]
            embed = embed.sort_values(by=['FlowSort'])

            combined = flow_block.merge(
                embed,
                how='outer',
                on=['FlowSort', 'FlowID', 'BlockID',
                    'FlowType']).sort_values(by=['FlowSort'])
            combined.drop(["variable", "EmbeddedData"], axis=1, inplace=True)
            combined.drop_duplicates(inplace=True)

        else:
            flow = flow[['FlowID', 'Type']].reset_index()
            flow.columns = ['FlowSort', 'FlowID', 'BlockID', 'FlowType']
            flow['Field'] = ''
            flow['Type'] = ''
            flow['Value'] = ''
            combined = flow

        blocks = pd.json_normalize(response['Blocks']).melt()
        blocks[["BlockID",
                "BlockSettings"]] = blocks.variable.str.split('.',
                                                              1,
                                                              expand=True)
        blocks = blocks[~blocks['BlockSettings'].str.contains('Options')
                        & ~blocks['BlockSettings'].str.contains('SubType')]

        blocks = blocks.pivot(index='BlockID',
                              columns='BlockSettings',
                              values='value')
        blocks = blocks['BlockElements'].apply(pd.Series).merge(
            blocks, right_index=True,
            left_index=True).drop(['BlockElements'], axis=1).melt(
                id_vars=['ID', 'Type', 'Description'],
                value_name="BlockElement").dropna()
        blocks.rename(columns={'ID': 'BlockID'}, inplace=True)

        blocks['ElementType'] = blocks['BlockElement']
        blocks['ElementType'] = blocks['ElementType'].apply(
            lambda x: x['Type'])
        blocks['QID'] = blocks['BlockElement'].apply(
            lambda x: x['QuestionID'] if 'QuestionID' in x else "")
        blocks = blocks.drop(['BlockElement'], axis=1)
        blocks.rename(
            columns=(lambda x: 'BlockElementSort' if x == 'variable' else
                     ('Block' + x
                      if (('Block' in x) == False and x != 'QID') else x)),
            inplace=True)
        blocks = combined.merge(blocks, on='BlockID', how='right')

        extract = question[(
            question.variable.str.contains('.Language.') == False)]
        extract[["QID", "QPath"]] = extract.variable.str.split('.',
                                                               1,
                                                               expand=True)
        extract[["QPath",
                 "ChoiceSetting"]] = extract.QPath.str.rsplit('.',
                                                              1,
                                                              expand=True)

        extract['value'] = extract.apply(
            lambda x: response['Questions'][x.QID]['Labels']
            if (x.QPath.startswith("Labels.") == True) else x['value'],
            axis=1)
        extract['ChoiceSetting'] = extract.apply(
            lambda x: None
            if (x.QPath.startswith("Labels.") == True) else x.ChoiceSetting,
            axis=1)
        extract['QPath'] = extract.apply(
            lambda x: "Labels"
            if (x.QPath.startswith("Labels.") == True) else x.QPath,
            axis=1)

        question_pvt = extract[(extract.ChoiceSetting.isnull() == True)]
        question_pvt = question_pvt.pivot_table(index=['QID'],
                                                columns=['QPath'],
                                                values='value',
                                                aggfunc='first').reset_index()

        question_settings = extract[
            (extract.QPath.str.contains("Choices.") == False)
            & (extract.QPath.str.contains("Answers.") == False)]

        choice_settings = question_settings[(
            question_settings.ChoiceSetting.str.replace(
                '-', '').str.isnumeric() == True)]

        question_settings = question_settings[(
            question_settings.ChoiceSetting.str.replace(
                '-', '').str.isnumeric() == False)]

        question_settings['QPath'] = question_settings.apply(
            lambda x: x['QPath'] + "." + x['ChoiceSetting'], axis=1)
        question_settings['QPath'] = question_settings.apply(
            lambda x: x['QPath'].split('.', 2)[0] + "." + x['QPath'].split(
                '.', 2)[2]
            if "AdditionalQuestions" in x['QPath'] else x['QPath'],
            axis=1)
        question_settings = question_settings.drop(
            columns=['variable', 'ChoiceSetting'])
        question_settings = question_settings.pivot_table(
            index=['QID'], columns=['QPath'], values='value',
            aggfunc='first').reset_index()

        question_pvt = question_pvt.merge(question_settings,
                                          how='left',
                                          on='QID')

        if (choice_settings.empty == False):
            choice_settings['CQID'] = choice_settings.apply(
                lambda x: x['QID'] + '-' + x['ChoiceSetting']
                if ((x['ChoiceSetting'] is not None) & (
                    (x['ChoiceSetting']).isnumeric())) else x['QID'],
                axis=1)
            choice_settings.drop(columns=['variable', 'QID'])
            choice_settings = choice_settings.pivot_table(
                index=['CQID'],
                columns=['QPath'],
                values='value',
                aggfunc='first').reset_index()

        answers = extract[(extract.QPath.str.contains("Answers.") == True)]
        if (answers.empty == False):
            answers[["QPath",
                     "CRecode"]] = answers.QPath.str.split('.', 1, expand=True)
            answers['CRecode'] = answers['CRecode'].apply(
                lambda x: '#' + x.split('.')[0] + '-' + x.split('.')[2]
                if "Answers" in x else x)
            answers['AnswerSort'] = 1
            answers['AnswerSort'] = answers.groupby(
                'QID')['AnswerSort'].cumsum()
            answers = answers.drop(columns=['variable', 'ChoiceSetting'])

        choices_pvt = extract[(extract.QPath.str.contains("Choices.") == True)]
        choices_pvt[["QPath",
                     "CRecode"]] = choices_pvt.QPath.str.split('.',
                                                               1,
                                                               expand=True)
        choices_pvt["IChoiceSetting"] = choices_pvt["CRecode"].apply(
            lambda x: None if x is None else (x.split('.', 1)[1]
                                              if x.count('.') > 0 else ""))
        choices_pvt["ChoiceSetting"] = choices_pvt.apply(
            lambda x: x['IChoiceSetting'] + "." + x['ChoiceSetting']
            if "Image" in str(x['IChoiceSetting']) else x['ChoiceSetting'],
            axis=1)
        choices_pvt["PGRGrpIdx"] = choices_pvt["CRecode"].apply(
            lambda x: None if x is None else x.split('.', 1)[0]
            if 'Choices' in x else None)
        choices_pvt["PGRChoiceIdx"] = choices_pvt["CRecode"].apply(
            lambda x: None if x is None else x.rsplit('.', 1)[1]
            if "Choices" in x else None)
        choices_pvt["CRecode"] = choices_pvt["CRecode"].apply(
            lambda x: None if x is None else (x.split('.', 1)[0]
                                              if x.count('.') > 0 else x))
        choices_pvt["CRecode"] = choices_pvt.apply(
            lambda x: x["CRecode"] if x["PGRChoiceIdx"] is None else "#" + x[
                "CRecode"] + "-" + x["PGRChoiceIdx"],
            axis=1)
        choices_pvt["CQID"] = choices_pvt.apply(
            lambda x: x["QID"]
            if x["CRecode"] is None else x["QID"] + x["CRecode"]
            if "#" in x["CRecode"] else x["QID"] + "-" + x["CRecode"],
            axis=1)
        choices_pvt = choices_pvt.pivot_table(index=['CQID', 'QID'],
                                              columns=['ChoiceSetting'],
                                              values='value',
                                              aggfunc='first').reset_index()

        if (choice_settings.empty == False):
            choices_pvt = choices_pvt.merge(choice_settings,
                                            on='CQID',
                                            how='left')

        choices_order = extract[(extract.QPath == "ChoiceOrder")]
        choices_order = choices_order.value.apply(pd.Series).merge(
            choices_order, right_index=True, left_index=True).drop(
                ["value", "QPath", "variable", "ChoiceSetting"],
                axis=1).melt(id_vars=['QID'], value_name="CRecode").dropna()
        choices_order.columns = ['QID', 'ChoiceOrder', 'CRecode']
        choices_order['CQID'] = choices_order['QID'] + "-" + choices_order[
            'CRecode'].astype(str)

        ### Combine SVF - Blocks - Questions - Choices - ChoiceOrder
        svFlattened = choices_pvt.merge(choices_order, how='left', on='CQID')
        svFlattened = svFlattened.drop(columns="QID_y")
        svFlattened = svFlattened.rename(columns={'QID_x': 'QID'})
        svFlattened = question_pvt.merge(svFlattened, how='outer', on='QID')
        svFlattened = blocks.merge(svFlattened, how='left', on='QID')

        svFlattened['QuestionText'] = svFlattened['QuestionText_Unsafe'].apply(
            lambda x: "" if x == "" else lhtmlclean.Cleaner(
                style=True).clean_html(lhtml.fromstring(str(x))).text_content(
                ).replace("nan", "").strip())
        svFlattened['Display'] = svFlattened['Display'].apply(
            lambda x: "" if x == "" else lhtmlclean.Cleaner(
                style=True).clean_html(lhtml.fromstring(str(x))).text_content(
                ).replace("nan", "").strip())

        svFlattened['CQID'] = svFlattened.apply(
            lambda x: x.CQID if "QID" in str(x.CQID) else x.Field
            if pd.isnull(x.Field) == False else x.QID
            if pd.isnull(x.QID) == False else "",
            axis=1)
        svFlattened = svFlattened.drop(
            columns=['AnswerOrder', 'ChoiceOrder_x'], errors='ignore')

        csvfilteredColumns = [
            'FlowSort', 'FlowID', 'BlockElementSort', 'BlockDescription',
            'QID', 'CQID', 'QuestionText', 'QuestionType', 'Selector',
            'SubSelector', 'DataExportTag', 'ChoiceDataExportTags_y',
            'Display', 'Image.Display', 'Image.ImageLocation',
            'VariableNaming', 'ChoiceOrder_y', 'CRecode'
        ]
        for x in csvfilteredColumns:
            if (x not in svFlattened.columns):
                svFlattened[x] = ''
        svFlattenedFiltered = svFlattened[csvfilteredColumns].drop_duplicates(
            subset='CQID', ignore_index=True)
        # only return filtered, do we need to return the result unfiltered?
        return svFlattenedFiltered

    def pull_results(self, survey_id):
        def pull_file(label, survey_id):

            file_type = lambda x: "With Labels" if label == True else "Without Labels"
            parameters = "{\"format\": \"csv\", \"useLabels\": "\
            + (str(label)).lower() + ", \"surveyId\": \""\
            + survey_id + "\"" + ", \"endDate\":\"" \
            + str(datetime.datetime.utcnow().isoformat()[0:19]) + "Z\"}"

            response = requests.post(url=self.response_url,
                                     headers=self.headers,
                                     data=parameters)
            responseFileID = response.json()["result"]["id"]
            if (responseFileID is not None):
                response = requests.get(url=self.response_url +
                                        responseFileID,
                                        headers=self.headers)
                responseFileStatus = response.json()["result"]["status"]

                while (responseFileStatus == "in progress"):
                    time.sleep(5)
                    response = requests.get(url=self.response_url +
                                            responseFileID,
                                            headers=self.headers)
                    responseFileStatus = response.json()["result"]["status"]
                    completion_rate = response.json(
                    )['result']['percentComplete']
                    print(
                        f"File Request ({file_type(label)}) - {completion_rate}%"
                    )
                if (responseFileStatus in self.failed_responses):
                    print("Error Network Issue / Failed Request : " + survey_id)
                responseFileDownload = response.json()["result"]["file"]
                response = requests.get(url=responseFileDownload,
                                        headers=self.headers)
            else:
                print('No Response file ID, please check the survey ID')

            with zipfile.ZipFile(io.BytesIO(response.content),
                                 mode='r') as file:
                download = file.read(list(file.NameToInfo.keys())[0]).decode()
                df = pd.read_csv(io.StringIO(download), low_memory=False)
            return df

        wlExport = pull_file(True, survey_id)
        nlExport = pull_file(False, survey_id)

        mdQID = pd.melt(wlExport.iloc[[1]])
        mdQID.columns = ["QRecode", "QID"]
        mdQID["QID"] = mdQID["QID"].apply(
            lambda x: json.loads(x.replace("'", "\""))["ImportId"])

        wlExport = wlExport.iloc[2:]
        nlExport = nlExport.iloc[2:]
        print("Exports are finished - Working on combining them...")

        wlExport = wlExport.rename(
            columns=lambda x: "ResponseID" if x == "ResponseId" else x)

        mdTxtResp = pd.melt(wlExport, id_vars=["ResponseID"])
        mdTxtResp.columns = ["ResponseID", "QRecode", "TxtRespAnswer"]

        #Join Back ResponseID Values
        mdRespIDs = pd.melt(wlExport, value_vars=["ResponseID"])
        mdRespIDs["TxtRespAnswer"] = mdRespIDs["value"]
        mdRespIDs.columns = ["QRecode", "ResponseID", "TxtRespAnswer"]

        def IsNumeric(x):
            try:
                float(x)
            except (ValueError):
                return ""
            return x

        #Merge Text w. Response ID Values
        ndTxtResp = mdTxtResp.merge(mdRespIDs, how='outer')
        nlExport = nlExport.rename(
            columns=lambda x: "ResponseID" if x == "ResponseId" else x)
        mdNumResp = pd.melt(nlExport, id_vars=["ResponseID"])
        mdNumResp.columns = ["ResponseID", "QRecode", "NumRespAnswer"]
        mdNumResp["NumRespAnswer"] = mdNumResp["NumRespAnswer"].apply(
            lambda x: IsNumeric(x))

        #Merge Text w. Num Resp Values
        ndTextNumResp = mdNumResp.merge(ndTxtResp, how='outer')
        #Merge Results w. QID // ndQColumns.merge for QIDs + QText
        ndResultsFlat = mdQID.merge(ndTextNumResp, how='outer')
        ndResultsFlat["SurveyID"] = survey_id

        #Use Recodes for QID for non Questions
        ndResultsFlat["QID"] = ndResultsFlat.apply(
            lambda x: x['QID'] if "QID" in str(x['QID']) else x['QRecode'],
            axis=1)
        #NumAns != TextAns QCID = QID + Recode
        ndResultsFlat["CQID"] = ndResultsFlat.apply(
            lambda x: x['QID'].rsplit("-", 1)[0] + "-" + str(x['NumRespAnswer']
                                                             ).split('.', 1)[0]
            if x['NumRespAnswer'] != x['TxtRespAnswer'] and "QID" in x["QID"]
            and pd.isnull(x['TxtRespAnswer']) == False and "TEXT" not in x[
                "QID"] and "#" not in x["QID"] and '-' not in x["QID"] else x[
                    'QID'].rsplit("-", 1)[0]
            if "#" in x['QID'] else x['QID'].replace('-Group', '').replace(
                '-Rank', '').replace('-TEXT', ''),
            axis=1)

        # Loop & Merge
        ndResultsFlat["CQID"] = ndResultsFlat.apply(
            lambda x: "QID" + x["CQID"].replace("-xyValues-x", "").replace(
                "-xyValues-y", "").split("_QID", 1)[1]
            if "_QID" in x["CQID"] else x["CQID"],
            axis=1)
        del wlExport, nlExport
        print("Done")
        return ndResultsFlat
