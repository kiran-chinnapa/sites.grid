import re



address_line ="\d{1,5}\s[A-Za-z].+"
zip_code ="\d{5}([-]|\s*)?(\d{4})?"
email = "[a-zA-Z0-9+_.-]+@[a-zA-Z.-]+"
phone = "(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}"
url = "((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)"
href_regex = "href=[\"\'](.*?)[\"\']"
capital_country= "[A-Z]{2,52}"
age_restriction= ".*\s\d{1,2}\s.*(\sage\s|\syears\s|\sunder\s|\sover\s|\sonly\s).*"
contact_info = r'(\d{1,5}\s\w.\s(\b\w*\b\s){1,2}\w*\.|\d{5}([-]|\s*)?(\d{4})?|[a-zA-Z0-9+_.-]+@[a-zA-Z.-]+|(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})'
us_address = r"/\b(p\.?\s?o\.?\b|post office|\d{2,5})\s*(?:\S\s*){8,50}(AK|Alaska|AL|Alabama|AR|Arkansas|AZ|Arizona|CA|California|CO|Colorado|CT|Connecticut|DC|Washington\sDC|Washington\D\.C\.|DE|Delaware|FL|Florida|GA|Georgia|GU|Guam|HI|Hawaii|IA|Iowa|ID|Idaho|IL|Illinois|IN|Indiana|KS|Kansas|KY|Kentucky|LA|Louisiana|MA|Massachusetts|MD|Maryland|ME|Maine|MI|Michigan|MN|Minnesota|MO|Missouri|MS|Mississippi|MT|Montana|NC|North\sCarolina|ND|North\sDakota|NE|New\sEngland|NH|New\sHampshire|NJ|New\sJersey|NM|New\sMexico|NV|Nevada|NY|New\sYork|OH|Ohio|OK|Oklahoma|OR|Oregon|PA|Pennsylvania|RI|Rhode\sIsland|SC|South\sCarolina|SD|South\sDakota|TN|Tennessee|TX|Texas|UT|Utah|VA|Virginia|VI|Virgin\sIslands|VT|Vermont|WA|Washington|WI|Wisconsin|WV|West\sVirginia|WY|Wyoming)(\s+|\&nbsp\;|\<(\S|\s){1,10}\>){1,5}\d{5}/i"
resume = r"(summary|experience|education|projects|skills|hobbies|portfolio|resume|curriculum|vitae)"
# resume = r"\n\W*Summary\W*\n|\n\W*SUMMARY\W*\n|\n\W*Experience\W*\n|\n\W*EXPERIENCE\W*\n|\n\W*Education\W*\n|\n\W*EDUCATION\W*\n|\n\W*Projects\W*\n|\n\W*PROJECTS\W*\n|\n\W*Skills\W*\n|\n\W*SKILLS\W*\n|\n\W*Hobbies\W*\n|\n\W*HOBBIES\W*\n|\n\W*Portfolio\W*\n|\n\W*PORTFOLIO\W*\n|\n\W*Resume\W*\n|\n\W*RESUME\W*\n|\n\W*Curriculum\W*\n|\n\W*CURRICULUM\W*\n|\n\W*Vitae\W*\n|\n\W*VITAE\W*\n"
# resume = r"\n\W*EDUCATION\W*\n|\n\W*SUMMARY\W*\n"
file_extension = r"(\.rtf|\.csv|\.xml|\.json|\.pdf|\.docx|\.doc|\.xlsx|\.xls|\.pptx|\.ppt|\.zip)"
tech_stack = r"(react|svelte|java|python|kafka|snowflake|postgres|jenkins|gitlab|nexus|pcf|terraform|kubernetes|aws|azure|gcp|linux)"
year_in_resume = r"\b\d{4}\b"
capitalized_words = r'\b[A-Z][a-z]+|\b[A-Z]\b'

def main():
    s = '''Everyday HOLDS 
    the 
    poSSibility 
    Of A Miracle
    '''
    new_s = ' '.join(re.findall(r'\b[A-Z][a-z]+|\b[A-Z]\b', s))
    print (new_s)
    # sentence ='''
    # 2116 haven rd apt d wilmington de 19809
    #      bro   EDUCATION    bro
    #     bro        EDUCATION bro
    #         SUMMARY
    #             SUMMARY
    # laurajefferson@bogusemail.com
    # '''
    # pattern = re.compile(resume)
    # matches = re.finditer(pattern, sentence)
    # print(any(True for _ in matches))
    # for match in matches:
    #     print(match)
    # src_address = re.search(address_line, sentence).group()
    # # geocode address format
    # # House Number, Street Direction, Street Name, Street Suffix, City, State, Zip, Country
    # # address is a String e.g. 'Berlin, Germany'
    # # addressdetails=True does the magic and gives you also the details
    # usa_address = usaddress.parse(src_address)
    # print(usa_address)
    # geo_add = []
    # if usa_address:
    #     [geo_add.append(add_tup[0].replace(',','')) for add_tup in usa_address]
    #     geo_add.append('USA')
    #     print(geo_add[-4:])

    # geo_address =', '.join(geo_add[-4:])
    # geolocator = Nominatim(user_agent="myGeocoder")
    # location = geolocator.geocode( geo_address, addressdetails=True)
    # if location is not None:
    #     print(location.raw)


if __name__ == "__main__":
    main()