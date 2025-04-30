import streamlit as st
import os
import pandas as pd
import tempfile
import traceback
from dotenv import load_dotenv
from scraper import extract_text_from_pdf, extract_company_names, save_to_csv, load_text_cache, save_text_cache
from google_maps_scraper import get_company_details, save_company_details_to_csv
from email_scraper import scrape_emails_with_selenium
from mongodb_utils import save_companies_to_mongodb, save_company_details_to_mongodb, register_user, get_user_extractions

# Load environment variables
load_dotenv()

def main():
    st.set_page_config(page_title="Company Name Extractor", page_icon="📄")
    
    # Initialize sidebar navigation
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "home"
    
    # Ask for user information first
    if 'user_name' not in st.session_state:
        st.title("Welcome to Company Name Extractor")
        st.markdown("Please enter your information to begin")
        
        with st.form("user_registration_form"):
            user_name = st.text_input("Your Name")
            email = st.text_input("Your Email (optional)")
            
            submitted = st.form_submit_button("Continue")
            if submitted and user_name:
                # Register user in MongoDB
                try:
                    user_id = register_user(user_name, email)
                    if user_id:
                        st.success(f"Welcome, {user_name}!")
                        st.session_state.user_name = user_name
                        st.session_state.user_email = email
                        st.session_state.user_id = user_id
                    else:
                        st.error("Failed to register user. Please try again.")
                except Exception as e:
                    st.error(f"Error registering user: {e}")
                    
                # We'll rerun to refresh the page after registration
                if 'user_name' in st.session_state:
                    st.rerun()
        return
    
    # Add sidebar navigation
    with st.sidebar:
        st.title(f"Hello, {st.session_state.user_name}")
        
        st.radio(
            "Navigation",
            ["Home", "My Extractions", "User Profile"],
            key="nav_selection",
            on_change=change_page
        )

    # Display the selected page
    if st.session_state.current_page == "home":
        display_home_page()
    elif st.session_state.current_page == "extractions":
        display_extractions_page()
    elif st.session_state.current_page == "profile":
        display_profile_page()

def change_page():
    """Change the current page based on navigation selection"""
    selection = st.session_state.nav_selection.lower().replace(" ", "_")
    
    # Map navigation options to page names
    page_mapping = {
        "home": "home",
        "my_extractions": "extractions",
        "user_profile": "profile"
    }
    
    st.session_state.current_page = page_mapping.get(selection, "home")

def display_profile_page():
    """Display user profile page"""
    st.title("User Profile")
    
    # Display current user information
    st.subheader("Your Information")
    
    user_name = st.session_state.user_name
    user_email = st.session_state.get("user_email", "")
    
    with st.form("update_profile_form"):
        updated_name = st.text_input("Name", value=user_name)
        updated_email = st.text_input("Email", value=user_email)
        
        submitted = st.form_submit_button("Update Profile")
        if submitted:
            # Update user in MongoDB
            try:
                user_id = register_user(updated_name, updated_email)
                if user_id:
                    st.success("Profile updated successfully!")
                    st.session_state.user_name = updated_name
                    st.session_state.user_email = updated_email
                    st.session_state.user_id = user_id
                    st.rerun()
                else:
                    st.error("Failed to update profile. Please try again.")
            except Exception as e:
                st.error(f"Error updating profile: {e}")

def display_extractions_page():
    """Display user extractions page"""
    st.title("My Extractions")
    
    # Get user extractions from MongoDB
    try:
        extractions = get_user_extractions(st.session_state.user_name)
        
        if extractions:
            companies = extractions.get("companies", [])
            company_details = extractions.get("company_details", [])
            
            # Display extraction statistics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Company Names Extracted", len(companies))
            with col2:
                st.metric("Company Details Extracted", len(company_details))
            
            # Display extractions in tabs
            tab1, tab2 = st.tabs(["Company Names", "Company Details"])
            
            with tab1:
                if companies:
                    # Create DataFrame for display
                    companies_df = pd.DataFrame([
                        {
                            "Company Name": doc["company_name"],
                            "Source": doc.get("source_pdf", ""),
                            "Date": doc.get("created_at", "")
                        } for doc in companies
                    ])
                    
                    st.dataframe(companies_df, use_container_width=True)
                    
                    # Download as CSV option
                    if st.button("Download Company Names as CSV"):
                        csv = companies_df.to_csv(index=False)
                        st.download_button(
                            label="Click to Download",
                            data=csv,
                            file_name="my_extracted_companies.csv",
                            mime="text/csv"
                        )
                else:
                    st.info("You haven't extracted any company names yet.")
            
            with tab2:
                if company_details:
                    # Create DataFrame for display
                    details_df = pd.DataFrame([
                        {
                            "Company Name": doc["company_name"],
                            "Emails": "; ".join(doc.get("emails", [])),
                            "Phones": "; ".join(doc.get("phones", [])),
                            "Website": doc.get("website", ""),
                            "Date": doc.get("created_at", "")
                        } for doc in company_details
                    ])
                    
                    st.dataframe(details_df, use_container_width=True)
                    
                    # Download as CSV option
                    if st.button("Download Company Details as CSV"):
                        csv = details_df.to_csv(index=False)
                        st.download_button(
                            label="Click to Download",
                            data=csv,
                            file_name="my_company_details.csv",
                            mime="text/csv"
                        )
                else:
                    st.info("You haven't extracted any company details yet.")
        else:
            st.info("You don't have any extractions yet. Start by extracting company names from PDFs.")
    except Exception as e:
        st.error(f"Error retrieving your extractions: {e}")
        st.error(traceback.format_exc())

def display_home_page():
    """Display the main extraction page"""
    # The main app functionality starts here
    st.title("PDF Company Name Extractor")
    st.markdown(f"Upload up to 10 PDF files to extract company names")
    
    # Create a temporary directory to store uploaded files
    temp_dir = tempfile.mkdtemp()
    
    # Session state to store extracted company names
    if 'extracted_companies' not in st.session_state:
        st.session_state.extracted_companies = []
    
    # Session state to store the CSV path
    if 'companies_csv_path' not in st.session_state:
        st.session_state.companies_csv_path = None
    
    # Session state to store company details CSV path
    if 'details_csv_path' not in st.session_state:
        st.session_state.details_csv_path = None
    
    # Session state for edited company names
    if 'edited_companies' not in st.session_state:
        st.session_state.edited_companies = []
    
    # Session state to control view mode
    if 'view_mode' not in st.session_state:
        st.session_state.view_mode = "upload"  # Possible values: "upload", "preview", "details"

    # File uploader for multiple files
    uploaded_files = st.file_uploader("Choose PDF files", type="pdf", accept_multiple_files=True)
    
    # Show warning if too many files uploaded
    if uploaded_files and len(uploaded_files) > 10:
        st.warning("You can only upload up to 10 files at once. Only the first 10 will be processed.")
        uploaded_files = uploaded_files[:10]
    
    # If files are uploaded, switch to upload mode
    if uploaded_files:
        st.session_state.view_mode = "upload"
        
        # Save all files to the temporary directory
        file_paths = []
        for uploaded_file in uploaded_files:
            temp_file_path = os.path.join(temp_dir, uploaded_file.name)
            with open(temp_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            file_paths.append(temp_file_path)
        
        st.success(f"{len(file_paths)} file(s) saved successfully!")
        
        # Place buttons side by side using columns
        col1, col2 = st.columns(2)
        
        # Process button in first column
        extract_clicked = col1.button("Extract Company Names")
        
        # Get details button in second column (only enable if companies are extracted)
        get_details_clicked = col2.button("Get Company Details", disabled=len(st.session_state.extracted_companies) == 0)
        
        if extract_clicked:
            # Load text cache
            text_cache = load_text_cache()
            
            # Initialize empty list to store all company names
            all_company_names = []
            company_names_by_file = {}
            
            # Create a progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Process each file
            for i, (file_path, uploaded_file) in enumerate(zip(file_paths, uploaded_files)):
                status_text.text(f"Processing {uploaded_file.name}... ({i+1}/{len(file_paths)})")
                
                with st.spinner(f"Processing {uploaded_file.name}..."):
                    # Extract text from PDF
                    extracted_text = extract_text_from_pdf(file_path, text_cache)
                    
                    if not extracted_text:
                        st.error(f"No text could be extracted from {uploaded_file.name}.")
                        company_names = []
                    else:
                        # Extract company names from the text
                        company_names = extract_company_names(extracted_text)
                        
                        if not company_names:
                            st.warning(f"No company names found in {uploaded_file.name}.")
                        else:
                            # Store company names for this file
                            company_names_by_file[uploaded_file.name] = company_names
                            all_company_names.extend(company_names)
                
                # Update progress
                progress_bar.progress((i + 1) / len(file_paths))
            
            # Hide progress elements when done
            progress_bar.empty()
            status_text.empty()
            
            # Display results
            if all_company_names:
                # Save all company names to CSV
                csv_path = os.path.join(temp_dir, "companylist.csv")
                
                # Create a new file with all company names
                is_new_file = True
                for filename, names in company_names_by_file.items():
                    save_to_csv(names, csv_path, filename, is_new_file=is_new_file)
                    # After first file, append to the CSV
                    is_new_file = False
                
                # Store unique companies in session state
                st.session_state.extracted_companies = sorted(list(set(all_company_names)))
                # Also initialize edited companies
                st.session_state.edited_companies = st.session_state.extracted_companies.copy()
                # Store CSV path in session state
                st.session_state.companies_csv_path = csv_path
                
                # Automatically save to MongoDB when CSV is created
                with st.spinner("Saving company names to MongoDB..."):
                    try:
                        success = save_companies_to_mongodb(
                            st.session_state.companies_csv_path,
                            st.session_state.user_name
                        )
                        if success:
                            st.success("Company names automatically saved to MongoDB!")
                        else:
                            st.error("Failed to save to MongoDB. Check the logs for more details.")
                    except Exception as e:
                        error_details = traceback.format_exc()
                        st.error(f"Error saving to MongoDB: {e}\n\nDetails: {error_details}")
                
                # Display aggregated results in tabs
                st.subheader("Extracted Company Names:")
                
                tab1, tab2 = st.tabs(["All Companies", "Companies by File"])
                
                with tab1:
                    # Show all unique company names
                    unique_companies = st.session_state.extracted_companies
                    df_all = pd.DataFrame({"Company Name": unique_companies})
                    st.dataframe(df_all)
                    st.success(f"Found {len(unique_companies)} unique company names across all files!")
                    
                    # Download button below the results
                    if st.session_state.companies_csv_path:
                        with open(st.session_state.companies_csv_path, "r") as file:
                            csv_data = file.read()
                        
                        st.download_button(
                            label="Download Company List as CSV",
                            data=csv_data,
                            file_name="company_list.csv",
                            mime="text/csv"
                        )
                    
                    # Remove separate MongoDB save button since we auto-save now
                    # Button to view in preview mode
                    if st.button("Edit Company Names"):
                        st.session_state.view_mode = "preview"
                        st.rerun()
                
                with tab2:
                    # Show companies by file
                    for filename, companies in company_names_by_file.items():
                        if companies:
                            st.write(f"**{filename}** - {len(companies)} companies found:")
                            df = pd.DataFrame({"Company Name": companies})
                            st.dataframe(df)
            else:
                st.error("No company names were found in any of the uploaded PDFs.")
                # Reset session state if no companies found
                st.session_state.extracted_companies = []
                st.session_state.companies_csv_path = None
            
            # Save updated text cache
            save_text_cache(text_cache)
        
        if get_details_clicked:
            st.session_state.get_details = True
            st.session_state.view_mode = "details"
            st.rerun()
    
    # Preview mode - show editable list of companies
    elif st.session_state.view_mode == "preview" and st.session_state.extracted_companies:
        st.subheader("Edit Extracted Companies")
        st.info(f"Found {len(st.session_state.extracted_companies)} companies. You can edit the list below before getting details.")
        
        # Initialize edited companies if needed
        if not st.session_state.edited_companies:
            st.session_state.edited_companies = st.session_state.extracted_companies.copy()
            
        # Display editable dataframe
        edited_df = pd.DataFrame({"Company Name": st.session_state.edited_companies})
        
        # Create columns for edit area and buttons
        preview_col1, preview_col2 = st.columns([3, 1])
        
        with preview_col1:
            edited_df = st.data_editor(
                edited_df,
                num_rows="dynamic",
                key="company_editor",
                use_container_width=True
            )
            # Update edited companies in session state
            st.session_state.edited_companies = edited_df["Company Name"].tolist()
            
        with preview_col2:
            # Button to save changes and create CSV
            if st.button("Save Changes"):
                # Update extracted companies
                st.session_state.extracted_companies = st.session_state.edited_companies
                
                # Create new CSV with updated companies
                csv_path = os.path.join(temp_dir, "companylist.csv")
                pd.DataFrame({"Company Name": st.session_state.extracted_companies}).to_csv(csv_path, index=False)
                st.session_state.companies_csv_path = csv_path
                
                st.success("Changes saved successfully!")
                
                # Automatically save to MongoDB when CSV is created
                with st.spinner("Saving updated company names to MongoDB..."):
                    try:
                        success = save_companies_to_mongodb(
                            st.session_state.companies_csv_path,
                            st.session_state.user_name
                        )
                        if success:
                            st.success("Updated company names automatically saved to MongoDB!")
                        else:
                            st.error("Failed to save to MongoDB. Check the logs for more details.")
                    except Exception as e:
                        error_details = traceback.format_exc()
                        st.error(f"Error saving to MongoDB: {e}\n\nDetails: {error_details}")
                
            # Download button
            if st.session_state.companies_csv_path:
                with open(st.session_state.companies_csv_path, "r") as file:
                    csv_data = file.read()
                
                st.download_button(
                    label="Download CSV",
                    data=csv_data,
                    file_name="company_list.csv",
                    mime="text/csv"
                )
                
            # Button to get company details
            if st.button("Get Company Details"):
                st.session_state.get_details = True
                st.session_state.view_mode = "details"
                st.rerun()
            
            # Button to return to upload mode
            if st.button("Upload New Files"):
                st.session_state.view_mode = "upload"
                st.rerun()

    # Show download buttons outside of processing logic if data is available
    if st.session_state.companies_csv_path or st.session_state.details_csv_path:
        st.subheader("Download Previous Results:")
        
        col1, col2 = st.columns(2)
        
        if st.session_state.companies_csv_path:
            with open(st.session_state.companies_csv_path, "r") as file:
                csv_data = file.read()
            
            col1.download_button(
                label="Download Company List",
                data=csv_data,
                file_name="company_list.csv",
                mime="text/csv"
            )
        
        if st.session_state.details_csv_path:
            with open(st.session_state.details_csv_path, "r", encoding='utf-8') as file:
                details_csv_data = file.read()
                
            col2.download_button(
                label="Download Company Details",
                data=details_csv_data,
                file_name="company_details.csv",
                mime="text/csv"
            )

    # Process company details section - will run in both "upload" and "details" view modes
    if ('extracted_companies' in st.session_state and st.session_state.extracted_companies and 
        (st.session_state.get_details if 'get_details' in st.session_state else False)):
        
        st.session_state.view_mode = "details"
        st.subheader("Getting Company Details")
        
        # Get unique company names - use the edited companies if available
        unique_companies = st.session_state.edited_companies if st.session_state.edited_companies else st.session_state.extracted_companies
        total_companies = len(unique_companies)
        
        # Company count selector
        if 'company_count' not in st.session_state:
            # Default to a reasonable number or all companies if less than 25
            default_count = min(25, total_companies)
            st.session_state.company_count = default_count
        
        # Create options for selector: 10, 25, 50, 100, 200, All
        count_options = [10, 25, 50, 100, 200, total_companies]
        # Filter out options that exceed the total company count
        count_options = [opt for opt in count_options if opt <= total_companies]
        if count_options[-1] != total_companies:
            count_options.append(total_companies)
            
        count_labels = [str(opt) if opt != total_companies else f"All ({total_companies})" for opt in count_options]
        
        # Create a mapping of labels to values
        count_mapping = dict(zip(count_labels, count_options))
        
        # Show selector
        selected_count_label = st.selectbox(
            f"Select how many companies to process (total: {total_companies}):",
            options=count_labels,
            index=count_options.index(min(st.session_state.company_count, total_companies))
        )
        
        # Get the selected count from the mapping
        selected_count = count_mapping[selected_count_label]
        st.session_state.company_count = selected_count
        
        # Show info with selected count
        st.info(f"Processing {selected_count} companies with priority to Indian companies.")
        
        # Confirm button 
        process_confirmed = st.button("Confirm and Process")
        
        if process_confirmed:
            # Limit to selected number of companies
            processing_companies = unique_companies[:selected_count]
            
            # Create status placeholder
            status_text = st.empty()
            
            # Define status callback function
            def update_status(message):
                status_text.text(message)
            
            # Get company details using Google Maps API
            with st.spinner(f"Getting details for {selected_count} companies..."):
                company_details = get_company_details(processing_companies, status_callback=update_status)
            
            if company_details:
                # Check which companies have websites but no emails
                websites_to_scrape = {}
                for company, details in company_details.items():
                    if details.get('website') and not details.get('emails'):
                        websites_to_scrape[company] = details.get('website')
                
                # If there are websites to scrape
                if websites_to_scrape:
                    with st.spinner("Scraping emails from websites..."):
                        status_text.text("Scraping emails from company websites. This may take a moment...")
                        
                        # Scrape emails from websites
                        for company, website in websites_to_scrape.items():
                            if website.startswith('http'):
                                try:
                                    status_text.text(f"Scraping emails from {website}...")
                                    emails = scrape_emails_with_selenium(website)
                                    if emails:
                                        company_details[company]['emails'] = emails
                                        status_text.text(f"Found {len(emails)} email(s) for {company}")
                                    else:
                                        status_text.text(f"No emails found for {company}")
                                except Exception as e:
                                    status_text.text(f"Error scraping emails from {website}: {str(e)}")
                            else:
                                # Try adding https:// prefix if not present
                                try:
                                    full_url = f"https://{website}"
                                    status_text.text(f"Scraping emails from {full_url}...")
                                    emails = scrape_emails_with_selenium(full_url)
                                    if emails:
                                        company_details[company]['emails'] = emails
                                        status_text.text(f"Found {len(emails)} email(s) for {company}")
                                    else:
                                        status_text.text(f"No emails found for {company}")
                                except Exception as e:
                                    status_text.text(f"Error scraping emails from {full_url}: {str(e)}")
                
                # Create a DataFrame for display
                details_rows = []
                for company, details in company_details.items():
                    details_rows.append({
                        'Company Name': company,
                        'Emails': '; '.join(details.get('emails', [])),
                        'Phone Numbers': '; '.join(details.get('phones', [])),
                        'Website': details.get('website', ''),
                        'Address': details.get('address', '')
                    })
                
                details_df = pd.DataFrame(details_rows)
                
                # Display the details
                st.subheader("Company Contact Details:")
                st.dataframe(details_df)
                
                # Save to CSV
                details_csv_path = os.path.join(temp_dir, "company_details.csv")
                save_company_details_to_csv(company_details, details_csv_path)
                
                # Store the CSV path in session state
                st.session_state.details_csv_path = details_csv_path
                
                # Automatically save company details to MongoDB
                with st.spinner("Saving company details to MongoDB..."):
                    try:
                        success = save_company_details_to_mongodb(
                            details_csv_path,
                            st.session_state.user_name
                        )
                        if success:
                            st.success("Company details automatically saved to MongoDB!")
                        else:
                            st.error("Failed to save details to MongoDB. Check the logs for more details.")
                    except Exception as e:
                        error_details = traceback.format_exc()
                        st.error(f"Error saving details to MongoDB: {e}\n\nDetails: {error_details}")
                
                # Provide download button below results
                with open(details_csv_path, "r", encoding='utf-8') as file:
                    details_csv_data = file.read()
                    
                st.download_button(
                    label="Download Company Details as CSV",
                    data=details_csv_data,
                    file_name="company_details.csv",
                    mime="text/csv"
                )

                # Reset the get_details flag
                st.session_state.get_details = False
                
                # Add buttons to return to other views
                col1, col2 = st.columns(2)
                if col1.button("Edit Companies"):
                    st.session_state.view_mode = "preview"
                    st.rerun()
                
                if col2.button("Upload New Files"):
                    st.session_state.view_mode = "upload"
                    st.rerun()
            else:
                st.error("No company details could be found.")
                # Reset flag
                st.session_state.get_details = False
        else:
            # If not confirmed yet, show a button to go back
            if st.button("Cancel"):
                st.session_state.get_details = False
                st.session_state.view_mode = "preview"
                st.rerun()

if __name__ == "__main__":
    main() 