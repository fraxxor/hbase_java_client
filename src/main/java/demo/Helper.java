package demo;

public class Helper {

	public static String getUuidStringFromNumber(int number) {
		int leadingZeros = 0;
		int j = 7;
		for (int i = 1; i <= 10000000; i = i * 10) {
			if (number >= i) {
				leadingZeros = j;
			}
			j--;
		}
		StringBuilder sb = new StringBuilder("uuid_");
		for (int i = 0; i < leadingZeros; i++) {
			sb.append("0");
		}
		sb.append(number);
		return sb.toString();
	}

	public static int getNumberFromUuidString(String uuid)
			throws NumberFormatException {
		String uuidRaw = uuid.replace("uuid_", "");
		return Integer.parseInt(uuidRaw);
	}

}
