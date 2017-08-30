package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class SelectWhere extends Main {
	public static SelectQuery selectQueryexecute;
	static Iterator outerit;
	SelectWhere() throws SQLException, IOException{
		whereClauseEvaluator();
	}
	public static void whereClauseEvaluator() throws SQLException, IOException{
		PrimitiveValue result;
//		LinkedHashMap<String, String> getSchemaDetails;
//		List<ArrayList<PrimitiveValue>> tempWorkingData;
//		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		//workingTuple = tupleRecord.toArray(new PrimitiveValue[0]);
		//System.out.println("Current Tuple " + tupleRecord.toString());
		try{
		result = expressionEvaluator.eval(whereclauseExpression);
		if(result.toBool() == true){
			bufferFromExternalClass = true;
			avgCount++;
			selectQueryexecute = new SelectQuery();
			//itemIndex++;
		}
		else{
			//Discard the tuple
			//System.out.println("Failure Happens!");
			whereCondition = false;
			if(lastRecord && orderPresent) {
				FileSplitandMerge.lastRecordsortandFlush();
			}
		}
	//}
	//SelectQuery.itemIndex = 0;
	bufferFromExternalClass = false;
		}
		catch(NullPointerException e){
			Iterator it = Main.workingRecordMapper.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry pair = (Map.Entry)it.next();
				System.out.println("Tuple in Error ");
				PrimitiveValue[] pvalues = (PrimitiveValue[]) pair.getValue();
				for(PrimitiveValue p: pvalues){
					System.out.print(p + ",");
				}
			}
		}
		//System.out.println(whereclauseExpression.toString());
		//if(result!=null){		
	}
}
