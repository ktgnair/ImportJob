package com.krishagni.importcsv.core;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.krishagni.catissueplus.core.administrative.domain.User;
import com.krishagni.catissueplus.core.biospecimen.events.CollectionProtocolRegistrationDetail;
import com.krishagni.catissueplus.core.biospecimen.events.ParticipantDetail;
import com.krishagni.catissueplus.core.biospecimen.events.PmiDetail;
import com.krishagni.catissueplus.core.biospecimen.events.VisitDetail;
import com.krishagni.catissueplus.core.biospecimen.services.CollectionProtocolRegistrationService;
import com.krishagni.catissueplus.core.biospecimen.services.VisitService;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.errors.ErrorType;
import com.krishagni.catissueplus.core.common.errors.OpenSpecimenException;
import com.krishagni.catissueplus.core.common.events.RequestEvent;
import com.krishagni.catissueplus.core.common.events.ResponseEvent;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;
import com.krishagni.catissueplus.core.common.util.EmailUtil;
import com.krishagni.catissueplus.core.common.util.Utility;
import com.krishagni.importcsv.datasource.DataSource;
import com.krishagni.importcsv.datasource.Impl.CsvFileDataSource;

public class CsvImporter {
	private final static String INPUT_FILE_NAME = "/Users/swapnil/Downloads/apache-tomcat-9.0.10/data/import.csv";
	
	private final static String REPORT_FILE_NAME = "/Users/swapnil/Downloads/apache-tomcat-9.0.10/data/csv-import-output.csv";
	
	private final static String DATE_FORMAT = "MM/dd/yyyy";
	
	private final static String FIRST_NAME = "First Name";
	
	private final static String LAST_NAME = "Last Name";
	
	private final static String PPID = "Treatment";
	
	private final static String MRN = "MRN";
	
	private final static String CP_SHORT_TITLE = "IRBNumber";
	
	private final static String VISIT_DATE = "Start Date";
	
	private final static String SITE_NAME = "Facility";
	
	private final static String VISIT = "Visit";
	
	private final static String VISIT_COMMENTS = "Visit Comments";
	
	private final static String DAY = "Day";

	private final static String MSK_IMPORT_JOB_STATUS = "msk_import_job_status";
	
	private final static Log logger = LogFactory.getLog(CsvImporter.class);
	
	private OpenSpecimenException ose;
	
	private DataSource dataSource;

	private CsvFileWriter reportFile;
	
	private int rowCount;
	
	private int recordsFailed;

	@Autowired
	private CollectionProtocolRegistrationService cprSvc;
	
	@Autowired
	private VisitService visitService;
	
	@PlusTransactional
	public void importCsv() {
		ose =  new OpenSpecimenException(ErrorType.USER_ERROR);
		dataSource = new CsvFileDataSource(INPUT_FILE_NAME);
		createOutputCsvFile();
		rowCount = 0;
		recordsFailed = 0;
			
		try {
		    isHeaderRowValid(dataSource); 
		    while (dataSource.hasNext()) {
		    	Record record = dataSource.nextRecord();
		    	importParticipant(record);
		    	rowCount++;
		    }
		    
		    if (ose.hasAnyErrors()) {
		    	sendImportReportMail();
		    	ose.checkAndThrow();
		    }
		} catch (OpenSpecimenException ose) {
		    logger.error("Error while parsing csv file in: \n" + ose.getMessage());
		} catch (Exception e) {
		    logger.error("Encountered server error: \n" + e.getMessage());
		}
		finally {
		    if (dataSource != null) {
		    	dataSource.close();
		    }
		    IOUtils.closeQuietly(reportFile);
		}
	}
	
//	private String getFile() {
//		File file = new File(ConfigUtil.getInstance().getDataDir() + File.separatorChar, FILE_NAME); 
//		return file.getAbsolutePath();
//	}
	
	private void importParticipant(Record record) throws ParseException {
		CollectionProtocolRegistrationDetail cprDetail = new CollectionProtocolRegistrationDetail();
		VisitDetail visitDetail = new VisitDetail();
		cprDetail.setCpShortTitle(record.getValue(CP_SHORT_TITLE));
		cprDetail.setParticipant(new ParticipantDetail());
		cprDetail.setRegistrationDate(new SimpleDateFormat(DATE_FORMAT).parse(record.getValue(VISIT_DATE)));
		
		// Adding participant Details
		cprDetail.setPpid(record.getValue(PPID));
		cprDetail.getParticipant().setFirstName(record.getValue(FIRST_NAME));
		cprDetail.getParticipant().setLastName(record.getValue(LAST_NAME));
	
		// Setting PMI
		cprDetail.getParticipant().setPhiAccess(true);
		PmiDetail pmi = new PmiDetail();
		
		pmi.setMrn(record.getValue(MRN));
		pmi.setSiteName(record.getValue(SITE_NAME));
		
		cprDetail.getParticipant().setPmi(pmi);
		
		// Setting Visit
		visitDetail.setCpShortTitle(record.getValue(CP_SHORT_TITLE));
		visitDetail.setPpid(record.getValue(PPID));
		visitDetail.setEventLabel(record.getValue(VISIT) + record.getValue(DAY));
		visitDetail.setComments(record.getValue(VISIT_COMMENTS));
		visitDetail.setVisitDate(new SimpleDateFormat(DATE_FORMAT).parse(record.getValue(VISIT_DATE)));
		
		ResponseEvent<CollectionProtocolRegistrationDetail> participantResponse = cprSvc.createRegistration(getRequest(cprDetail));
		ResponseEvent<VisitDetail> visitResponse = visitService.addVisit(getRequest(visitDetail));
		
		if (participantResponse.getError() != null) {
			participantResponse.getError().getErrors().forEach(error -> ose.addError(error.error(), error.params()));
			addRowToReport(record, participantResponse.getError());
		}
		
		if (visitResponse.getError() != null) {
			visitResponse.getError().getErrors().forEach(error -> ose.addError(error.error(), error.params()));
			addRowToReport(record, visitResponse.getError());
		}
			
		if (participantResponse.getError() != null) {
			participantResponse.getError().getErrors().forEach(error -> ose.addError(error.error(), error.params()));
			addRowToReport(record, participantResponse.getError());
		}
	}
	
	private void addRowToReport(Record record, OpenSpecimenException error) {
		List<String> data = getRow(record);
		data.add(error.getMessage());
		reportFile.writeNext(data.toArray(new String[data.size()]));
		recordsFailed++;
	}

	private List<String> getRow(Record record) {
		List<String> headers = getCsvHeaders();
		List<String> values = new ArrayList<>();
		headers.forEach(header -> values.add(record.getValue(header)));
		
		return values;
	}

	private void isHeaderRowValid(DataSource dataSource) throws Exception {
		String[] csvHeaderRow = dataSource.getHeader();
		List<String> expectedHeader = getCsvHeaders();
		
		for (String header : csvHeaderRow) {
			if (!expectedHeader.contains(header)) {
				throw new Exception("Could not parse the file because the headers of the CSV file is not as expected.");
			}
		}
	}
	
	private List<String> getCsvHeaders() {
		List<String> headers = new ArrayList<String>();
		
		headers.add(FIRST_NAME);
		headers.add(LAST_NAME);
		headers.add(PPID);
		headers.add(MRN);
		headers.add(CP_SHORT_TITLE);
		headers.add(VISIT_DATE);
		headers.add(SITE_NAME);
		headers.add(VISIT);
		headers.add(DAY);
		headers.add(VISIT_COMMENTS);
		
		return headers;
	}
	
	private void sendImportReportMail() {
		String date = Utility.getDateString(Calendar.getInstance().getTime());

		Map<String, Object> emailProps = new HashMap<>();
		emailProps.put("$subject", new String[] {date});
		emailProps.put("date", date);
		emailProps.put("ccAdmin", false);
		emailProps.putAll(getMailprops());

		List<User> rcpts = new ArrayList<>();

		String itAdminEmailId = ConfigUtil.getInstance().getItAdminEmailId();
		if (StringUtils.isNotBlank(itAdminEmailId)) {
			User itAdmin = new User();
			itAdmin.setFirstName("IT");
			itAdmin.setLastName("Admin");
			itAdmin.setEmailAddress(itAdminEmailId);
			rcpts.add(itAdmin);
		}

		String emailTmpl = MSK_IMPORT_JOB_STATUS;
		File[] attachments = new File[] {new File(REPORT_FILE_NAME)};
		for (User user : rcpts) {
			emailProps.put("rcpt", user);
			EmailUtil.getInstance().sendEmail(emailTmpl, new String[] {user.getEmailAddress()}, attachments, emailProps);
		}
	}
	
	private Map<String, Object> getMailprops() {
		Map<String, Object> result = new HashMap<>();
		
		result.put("totalRecords", this.rowCount);
		result.put("failedRecords", this.recordsFailed);
		result.put("passedRecords", this.rowCount - this.recordsFailed);
		result.put("jobID", "Not Specified");
		result.put("filename", INPUT_FILE_NAME);
		
		return result;
	}
	
	private void createOutputCsvFile() {
		reportFile = CsvFileWriter.createCsvFileWriter(new File(REPORT_FILE_NAME));
		List<String> headers = getCsvHeaders();
		headers.add("ERROR");
		reportFile.writeNext(headers.toArray(new String[headers.size()]));
	}
	
	private <T> RequestEvent<T> getRequest(T payload) {
		return new RequestEvent<T>(payload);
	}
}
