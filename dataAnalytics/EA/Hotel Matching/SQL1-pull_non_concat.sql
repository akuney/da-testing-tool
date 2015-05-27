
SELECT hotel_property_ID, 
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(
														LOWER(
																	CASE ISNULL(NAME) WHEN 1 THEN ' ' ELSE name END
															),',',''
														),'\n',' '
													),'.',''
												),'the',''
											),'hotel',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as name,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
															(CASE ISNULL(state) WHEN 1 THEN ' ' ELSE state END)
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as state,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
														(CASE ISNULL(country) WHEN 1 THEN ' ' ELSE country END) 
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as country,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
														(CASE ISNULL(address_line_1) WHEN 1 THEN ' ' ELSE address_line_1 END) 
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as address_line_1,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
														(CASE ISNULL(address_line_2) WHEN 1 THEN ' ' ELSE address_line_2 END) 
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as address_line_2,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
														(CASE ISNULL(city_name) WHEN 1 THEN ' ' ELSE city_name END) 
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as city_name,
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												LOWER(
														(CASE ISNULL(zip) WHEN 1 THEN ' ' ELSE zip END) 
													),',',''
												),'\n',' '
											),'.',''
										),'null', ''
									),'#',''
								),'s\n',' '
							),'s\\n',' '
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as zip
FROM publishers_hotel_properties
where hotel_property_ID is not null
ORDER BY hotel_property_ID


