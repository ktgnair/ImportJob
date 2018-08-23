package com.krishagni.importcsv.core;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

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
import com.krishagni.importcsv.datasource.DataSource;
import com.krishagni.importcsv.datasource.Impl.CsvFileDataSource;

public class CsvImporter {
	private final static String FILE_NAME = "import.csv";
	
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
	
	private final static Log logger = LogFactory.getLog(CsvImporter.class);
	
	private OpenSpecimenException ose;
	
	private DataSource dataSource;
	
	@Autowired
	private CollectionProtocolRegistrationService cprSvc;
	
	@Autowired
	private VisitService visitService;
	
	@PlusTransactional
	public void importCsv() {
		ose = new OpenSpecimenException(ErrorType.USER_ERROR);
		dataSource = new CsvFileDataSource(getFile());
		
		try {
		    isHeaderRowValid(dataSource); 
		    while (dataSource.hasNext()) {
		    	Record record = dataSource.nextRecord();
		    	importParticipant(record);
		    }
		    ose.checkAndThrow();
		} catch (OpenSpecimenException ose) {
		    logger.error("Error while parsing csv file in: \n" + ose.getMessage());
		} catch (Exception e) {
		    logger.error("Encountered server error: \n" + e.getMessage());
		}
		finally {
		    if (dataSource != null) {
		    	dataSource.close();
		    }
		}
	}
	
	private String getFile() {
		File file = new File(ConfigUtil.getInstance().getDataDir() + File.separatorChar, FILE_NAME); 
		return file.getAbsolutePath();
	}
	
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
		}
		
		if (visitResponse.getError() != null) {
			visitResponse.getError().getErrors().forEach(error -> ose.addError(error.error(), error.params()));
		}
	}
	
	private void isHeaderRowValid(DataSource dataSource) throws Exception {
		String[] csvHeaderRow = dataSource.getHeader();
		List<String> expectedHeader = new ArrayList<String>();
		
		expectedHeader.add(FIRST_NAME);
		expectedHeader.add(LAST_NAME);
		expectedHeader.add(PPID);
		expectedHeader.add(MRN);
		expectedHeader.add(CP_SHORT_TITLE);
		expectedHeader.add(VISIT_DATE);
		expectedHeader.add(SITE_NAME);
		expectedHeader.add(VISIT);
		expectedHeader.add(DAY);
		expectedHeader.add(VISIT_COMMENTS);
		
		for (String header : csvHeaderRow) {
			if (!expectedHeader.contains(header)) {
				throw new Exception("Could not parse the file because the headers of the CSV file is not as expected.");
			}
		}
	}
	
	private <T> RequestEvent<T> getRequest(T payload) {
		return new RequestEvent<T>(payload);
	}
}
